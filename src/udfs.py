# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata, sys

import math
@udf(returnType=ArrayType(StringType()))
def extract_framework_plattform(mo_ta_cong_viec,yeu_cau_ung_vien):
    text = (mo_ta_cong_viec or "") + " " + (yeu_cau_ung_vien or "")
    result = []

    for framework, keywords in framework_platforms.items():
        search_terms = keywords
        for keyword in search_terms:
            pattern = r'\b' + re.escape(keyword) + r'(?=\W|$)'
            if re.search(pattern, text, re.IGNORECASE):
                result.append(framework)
                break
    return result

@udf(returnType=ArrayType(StringType()))
def extract_language(mo_ta_cong_viec, yeu_cau_ung_vien):
    text = (mo_ta_cong_viec or '') + ' ' + (yeu_cau_ung_vien or '')
    result = []

    for language, keywords in languages.items():
        search_terms = keywords
        for keyword in search_terms:
            pattern = r'\b' + re.escape(keyword) + r'(?=\W|$)'
            if re.search(pattern, text, re.IGNORECASE):
                result.append(language)
                break
    return result

@udf(returnType=ArrayType(StringType()))
def extract_knowledge(mo_ta_cong_viec,yeu_cau_ung_vien):
    result = set()
    text = (mo_ta_cong_viec or "") + " " + (yeu_cau_ung_vien or "")

    for group, keywords in knowledge_groups.items():
        for keyword in keywords:
            pattern = r'\b' + re.escape(keyword) + r'(?=\W|$)'
            if re.search(pattern, text, re.IGNORECASE):
                result.add(group)
                break
    return list(result)

@udf(returnType=ArrayType(StringType()))
def extract_workplace(dia_diem_lam_viec):
    def remove_accents(text):
        if text is None:
            return ""
        if sys.version_info[0] >= 3:
            return ''.join(
                c for c in unicodedata.normalize('NFD', text.decode('utf-8'))
                if unicodedata.category(c) != 'Mn'
            )
        else:
            if not isinstance(text, unicode):
                text = unicode(text, 'utf-8')
            return ''.join(
                c for c in unicodedata.normalize('NFD', text)
                if unicodedata.category(c) != 'Mn'
            )
    
    result = set()
    text = (dia_diem_lam_viec or "")
    
    for province, keywords in workplaces.items():
        for keyword in keywords:
            keyword_no_accent = remove_accents(keyword).lower()
            pattern = r'\b' + re.escape(keyword_no_accent) + r'(?=\W|$)'
            if re.search(pattern, remove_accents(text).encode('utf-8').lower()):
                result.add(province)
                break
                
    return list(result)

@udf(returnType=ArrayType(StringType()))
def extract_design_pattern(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [design_pattern for design_pattern in design_patterns if re.search(design_pattern, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(IntegerType()))
def normalize_salary(quyen_loi):
    BIN_SIZE=5
    def extract_salary(quyen_loi):
        '''
        Return a list of salary patterns found in raw data

        Parameters
        ----------
        quyen_loi : quyen_loi field in raw data
        '''
        salaries = []
        for pattern in salary_patterns:
            salaries.extend(re.findall(pattern, unicodedata.normalize('NFKC', quyen_loi), re.IGNORECASE))
        return salaries

    def sal_to_bin_list(sal):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        sal : salary value
        '''
        sal = int(sal/BIN_SIZE)
        if sal<int(100/BIN_SIZE):
            return [BIN_SIZE*sal]
        else :
            return [100]

    def range_to_bin_list(start, end):
        '''
        Return a list of bin containing salary range

        Parameters
        ----------
        start : the start of salary range
        end : the end of salary range
        '''
        start = int(start/BIN_SIZE)
        end = int(end/BIN_SIZE)
        if end >= int(100/BIN_SIZE):
            end=int(100/BIN_SIZE)
        return [BIN_SIZE*i for i in range(start,end+1)]


    def dollar_to_vnd(dollar):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        dollar : salary value in dollar unit
        '''
        return sal_to_bin_list(math.floor(dollar*23/1000))

    def dollar_handle(currency):
        '''
        Handle currency
        If currency is in dollar unit, returns the salary bins
        Otherwise returns None

        Parameter
        ---------
        currency : string of salary pattern
        '''
        if not currency.__contains__("$"):
            if not currency.__contains__("USD"):
                if not currency.__contains__("usd"):
                    return None
                else :
                    ext_curr= currency.replace("usd","")
            else :
                ext_curr = currency.replace("USD","")
        elif (currency.startswith("$")):
            ext_curr = currency[1:]
        else :
            ext_curr = currency[:-1]
        ext_curr= ext_curr.replace(".","")
        try :
            val_curr = int(ext_curr)
            return dollar_to_vnd(val_curr)
        except ValueError:
            return None

    def normalize_vnd(vnd):
        '''
        Return normalized currency in VND unit
        Normalize currency is a string of currency in milion VND unit
        The postfix such as Triệu, triệu, M, m,... is removed

        Parameters
        ----------
        vnd : string of salary in vnd unit
        '''
        try :
            vnd = unicodedata.normalize('NFKC', vnd)
            mill = "000000"
            norm_vnd = vnd.replace("triệu",mill).replace("Triệu",mill)\
            .replace("TRIỆU",mill).replace("m",mill).replace("M",mill)\
            .replace(".","").replace(" ","").replace(",","")
        
            vnd = math.floor(int(norm_vnd)/1000000)
            return vnd
        except ValueError:
            print("Value Error while converting ", vnd)
            return None

    def vnd_handle(ori_range_list):
        '''
        Handle currency, returns the salary bins
        The currency must be preprocessed and returned None by dollar_handle()
        The currency must be stripped and splitted by "-" to become a list
        
        Parameters
        ----------
        ori_range_list : the range of salary (a list containing at most 2 element)
        '''
        if (len(ori_range_list)==1):
            sal = normalize_vnd(ori_range_list[0])
            if sal!=None:
                return sal_to_bin_list(sal)
        else :
            try :
                start = int(ori_range_list[0].strip().replace(".","").replace(",",""))
                end = normalize_vnd(ori_range_list[1])
                if end!=None :
                    return range_to_bin_list(start,end)
                else :
                    print("Error converting end ",ori_range_list[1]," with start ",ori_range_list[0])
            except ValueError:
                print("Error Converting Start ",ori_range_list[0]," with end ",ori_range_list[1])
        # return [0]*11
        return None

    def salary_handle(currency):
        '''
        Handle currency
        Return salary bin

        Parameters
        ----------
        currency : a string
        '''
        range_val = dollar_handle(currency)
        if (range_val == None):
            splitted_currency = currency.strip().strip("-").split("-")
            range_val = vnd_handle(splitted_currency)
        return range_val

    salaries = extract_salary(quyen_loi)
    bin_set = set()
    for sal in salaries:
        sal_bins = salary_handle(sal)
        if sal_bins!= None and sal_bins!=[]:
            bin_set = bin_set.union(tuple(sal_bins))
    return sorted(list(bin_set))


design_patterns = ["MVC"," Singleton"," WPF", " MVVM","Session Facade", " DAO ", "OOA/OOD","Factory Pattern", 'Microservice']

workplaces = {
    "An Giang": ["An Giang"],
    "Ba Ria - Vung Tau": ["Bà Rịa", "Vũng Tàu"],
    "Bac Lieu": ["Bạc Liêu"],
    "Bac Giang": ["Bắc Giang"],
    "Bac Kan": ["Bắc Kạn"],
    "Bac Ninh": ["Bắc Ninh"],
    "Ben Tre": ["Bến Tre"],
    "Binh Duong": ["Bình Dương"],
    "Binh Dinh": ["Bình Định"],
    "Binh Phuoc": ["Bình Phước"],
    "Binh Thuan": ["Bình Thuận"],
    "Ca Mau": ["Cà Mau"],
    "Cao Bang": ["Cao Bằng"],
    "Can Tho": ["Cần Thơ"],
    "Đa Nang": ["Đà Nẵng"],
    "Dak Lak": ["Đắk Lắk"],
    "Dak Nong": ["Đắk Nông"],
    "Dien Bien": ["Điện Biên"],
    "Dong Nai": ["Đồng Nai"],
    "Dong Thap": ["Đồng Tháp"],
    "Gia Lai": ["Gia Lai"],
    "Ha Giang": ["Hà Giang"],
    "Ha Nam": ["Hà Nam"],
    "Ha Noi": ["Hà Nội", "ha noi", "hanoi"],
    "Ha Tinh": ["Hà Tĩnh"],
    "Hai Duong": ["Hải Dương"],
    "Hai Phong": ["Hải Phòng"],
    "Hau Giang": ["Hậu Giang"],
    "TP. Ho Chi Minh": ["Hồ Chí Minh", "HCM", "TP HCM"],
    "Hoa Binh": ["Hòa Bình"],
    "Hung Yen": ["Hưng Yên"],
    "Khanh Hoa": ["Khánh Hòa"],
    "Kien Giang": ["Kiên Giang"],
    "Kon Tum": ["Kon Tum"],
    "Lai Chau": ["Lai Châu"],
    "Lam Dong": ["Lâm Đồng"],
    "Lang Son": ["Lạng Sơn"],
    "Lao Cai": ["Lào Cai"],
    "Long An": ["Long An"],
    "Nam Dinh": ["Nam Định"],
    "Nghe An": ["Nghệ An"],
    "Ninh Binh": ["Ninh Bình"],
    "Ninh Thuan": ["Ninh Thuận"],
    "Phu Tho": ["Phú Thọ"],
    "Phu Yen": ["Phú Yên"],
    "Quang Binh": ["Quảng Bình"],
    "Quang Nam": ["Quảng Nam"],
    "Quang Ngai": ["Quảng Ngãi"],
    "Quang Ninh": ["Quảng Ninh"],
    "Quang Tri": ["Quảng Trị"],
    "Soc Trang": ["Sóc Trăng"],
    "Son La": ["Sơn La"],
    "Tay Ninh": ["Tây Ninh"],
    "Thai Binh": ["Thái Bình"],
    "Thai Nguyen": ["Thái Nguyên"],
    "Thanh Hoa": ["Thanh Hóa"],
    "Thua Thien Hue": ["Thừa Thiên Huế"],
    "Tien Giang": ["Tiền Giang"],
    "Tra Vinh": ["Trà Vinh"],
    "Tuyen Quang": ["Tuyên Quang"],
    "Vinh Long": ["Vĩnh Long"],
    "Vinh Phuc": ["Vĩnh Phúc"],
    "Yen Bai": ["Yên Bái"]
}

framework_platforms = {
    "Rails": ["Rails", "Ruby on Rails"],
    "Spring": ["Spring", "Spring Boot"],
    "Django": ["Django"],
    "ReactJS": ["Reactjs", "React.js", "React"],
    "Struts": ["Struts"],
    "Webpack": ["Webpack"],
    "Vue": ["Vue", "Vue.js"],
    "Meteor": ["METEOR"],
    "Rancher": ["Rancher"],
    "Angular": ["Angular", "AngularJS"],
    "Flask": ["Flask"],
    "ASP.NET": ["ASP.NET", "ASP NET", "ASPNet", ".NET", "dotnet"],
    "Zend": ["Zend"],
    "Symfony": ["Symfony"],
    "Express": ["Express", "ExpressJS", "Express.js"],
    "Google Protobuf": ["Google Protobuf", "Protobuf"],
    "CakePHP": ["CakePHP"],
    "Hibernate": ["Hibernate"],
    "Redux": ["Redux"],
    "CodeIgniter": ["CodeIgniter"],
    "Laravel": ["Laravel"]
}

knowledge_groups = {
    "Blockchain Crypto": ["blockchains", "crypto", "NFT", "smart contract", "Defi"],
    "Microsoft Office": ["Word", "Excel", "Powerpoint", "Office"],
    "AI": [" AI", "Machine Learning", "Data mining", "Chatbot", "data analys"],
    "Tester": ["Black Box", "tester", "White Box", "Unit Test", "TestRail", "kiểm thử"],
    "Version Control": ["SVN", "SCM", "Git"],
    "Hardware": ["lắp đặt", "sửa chữa", "phần cứng", "router", "Corel Draw", "Switch"],
    "Graphic": ["Illustrator", "Photoshop", "Animate", "UI/UX", "Sketch", "interaction design", "đồ họa"],
    "Mobile": ["Android", "IOS", "Mobile"],
    "Web": ["frontend", "backend", "java web", "Wordpress", "Front-end", "Restful"],
    "Devops": ["DevOps", "Jenkins", "CI/CD", "distributed system", "multithreading", "async", "WebSocket"],
    "Network": ["networking", "mạng máy tính", "quản trị mạng", "TCP", "HTTP"],
    "Cyber Security": ["XSS", "cybersecurity", "cyber security", "an ninh mạng"],
    "Marketing": ["sale", "Consult", "Marketing", "chạy quảng cáo", "đánh giá chất lượng"],
    "Database": ["CSDL", "SQL", "MongoDB", "Database"],
    "OS": ["Linux", "Windows", "macOS"],
}

languages = {
    "Python": ["python"],
    "PHP": ["php", "laravel", "symfony"],
    "Red": ["red"],
    "JavaScript": ["javascript", "js", "node.js", "nodejs", "vue.js", "react.js", "angular.js", "vuejs", "reactjs", "angularjs"],
    "Swift": ["swift"],
    "TypeScript": ["typescript", "ts"],
    "Scala": ["scala"],
    "Scratch": ["scratch"],
    "C/C++": ["C/C++", "C++", "CPP", "c++"],
    "Dart": ["dart", "flutter"],
    "Java": ["java", "spring", "springboot"],
    "Go": ["golang"],
    "Kotlin": ["kotlin"],
    "Rust": ["rust"],
    "Opa": ["opa"],
    "Ruby": ["ruby", "rails", "ruby on rails"],
    "Groovy": ["groovy"],
    "PowerShell": ["powershell"],
    "CUDA": ["cuda"],
    "Hack": ["hack"],
    "C#": ["c#", "c sharp", ".net", "dotnet"]
}

salary_patterns = ["lương(?:từ| )+ ((?:\d+|\.)+)", "((?:\d+|\.|-| )+(?:triệu| )+)đồng",
                   "(?:\d|\.|,)+.000.000", "(?:\d+| |-)+\d+ *(?:triệu|m)", "\$(?:\d+|\.)", "(?:\d+|\.)+ *(?:USD|\$)+",
                   "(?:\d|\.|,)+,000,000"]
