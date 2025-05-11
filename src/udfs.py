# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata

import math
@udf(returnType=ArrayType(StringType()))
def extract_framework_plattform(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [framework for framework in framework_plattforms if re.search(framework, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_language(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [language for language in languages if re.search(language.replace("+", "\+").replace("(", "\(").replace(")", "\)"), mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_knowledge(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [knowledge for knowledge in knowledges if re.search(knowledge, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

def broadcast_labeled_knowledges(sc,labeled_knowledges):
    '''
    broadcast the mapped of labeled_knowledges to group data in knowledge field
    '''
    global mapped_knowledge
    mapped_knowledge = sc.broadcast(labeled_knowledges)

@udf(returnType=StringType())
def labeling_knowledge(knowledge):
    try :
        return mapped_knowledge.value[knowledge]
    except :
        return None

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


framework_plattforms = ['Docker', 'OSP', 'Premiere', 'directAdmin', 'typography', 'Prometheus', 'visual weight', 'Kubernetes', 'JDBC', 'UnitTest',
            'Servlets', 'cPanel', 'MySQL', '.NET', 'Ruby on Rails', 'JSP', 'IdentityServer', 'VoIP', 'AdobeXD', 'CMake', 'Autocad',
            'Spring', 'Django', 'CRM', 'K8S', 'Nginx', 'firmware', 'Google Trend', 'psd', 'CSRF', 'Reactjs', 'Struts', 'WebSocket',
            'Webpack', 'Spine', 'Vue', 'METEOR', 'Rancher', 'VFX', 'node js', 'Angular', 'Flask', 'ASP.NET', 'Google Analystics', 'Zend',
            'Symfony', 'Express', 'Google Protobuf', 'J2EE', 'Ansible', 'WebForm', 'Videoscribe', 'CakePHP', 'Hibernate', 'Git', 'Oracle',
            'Plesk', 'Log4j', 'JSON', 'Visio', 'Grafana', 'SDLC', ' EELinux', ' Redis', 'Redux', 'WinForm', 'Figma', 'CodeIgniter',
            'Power BI', 'Bootstrap', 'WPF', 'Aerospike', 'bash shell', 'Laravel', 'SQL Server']

design_patterns=["design pattern","MVC"," Singleton"," WPF", " MVVM","Session Facade", " DAO ", "OOA/OOD","Factory Pattern",
                'Microservice']

knowledges = ['game', 'Jira', 'lắp đặt', 'interaction design', 'đồ họa', 'DevOps', ' AI ', ' async', 'Quality Assurance',
               ' Security ', 'Google Drive', 'NFT', 'mạng máy tính', 'Wordpress', 'Machine Learning', 'Consult', 'White Box', 'sale',
               'kiểm thử', 'đánh giá chất lượng', 'networking', 'distributed system', 'UI/UX', 'Windows', 'Unit Test',
               'Jenkins', 'Chatbot', 'quản trị mạng', 'Solidity', 'tester', 'Corel Draw', 'Illustrator', 'Git', 'Android',
               'Black Box', 'Office', 'chạy quảng cáo', 'Unix', 'IT Support', 'Data mining', 'data analys', 'cấu trúc dữ liệu', 'Switch',
               ' TCP', 'qa', 'Animate', 'crypto', 'CI/CD', 'Defi', 'frontend', 'sửa chữa', 'SVN', 'phần cứng', ' sync',
               'Powerpoint', 'smart contract', 'Linux', 'SCM', 'backend', 'Marketing', 'XSS', 'Photoshop', 'HTTP', 'Word', 'router', 'IOS',
               'WebSocket', 'thuật toán', 'TestRail', 'CSDL', 'Sketch', 'blockchains', 'multithreading', 'hướng đối tượng', 'Front-end',

               'latex', 'Restful', 'Subversion', 'java web', 'Mobile', 'Excel']
knowledge_groups={
    "blockchain_crypto":["blockchains","crypto","NFT","smart contract","Solidity", "Defi",'XSS', " Security " ]    ,
    "office":["Word", "Excel","Powerpoint","Office"],
    "AI":[" AI", "Machine Learning","Data mining", 'Chatbot',"data analys"],
    "tester":["Black Box", "tester", 'White Box', "Unit Test",'TestRail', "kiểm thử"],
    "programming_basic": ["cấu trúc dữ liệu","thuật toán","OOP","hướng đối tượng"],
    "version_control":["SVN", "SCM","Git"],
    "hard_ware":[ "lắp đặt", "sửa chữa", "phần cứng","router",'Corel Draw',"Switch"],
    "photoshop": ["Illustrator","Photoshop","Animate"]
}

labeled_knowledges={' AI': 'AI', 'Machine Learning': 'AI', 'Data mining': 'AI', 'Chatbot': 'AI', 'data analys': 'AI',
                    'blockchains': 'blockchain_crypto', 'crypto': 'blockchain_crypto', 'NFT': 'blockchain_crypto',
                    'smart contract': 'blockchain_crypto', 'Solidity': 'blockchain_crypto', 'Defi': 'blockchain_crypto',
                    'XSS': 'blockchain_crypto', ' Security ': 'blockchain_crypto',
                    'lắp đặt': 'hardware', 'sửa chữa': 'hardware', 'phần cứng': 'hardware', 'router': 'hardware',
                    'Corel Draw': 'hardware', 'Switch': 'hardware',
                    'Word': 'office', 'Excel': 'office', 'Powerpoint': 'office', 'Office': 'office',
                    'Illustrator': 'photoshop', 'Photoshop': 'photoshop', 'Animate': 'photoshop',
                    'cấu trúc dữ liệu': 'programming_basic', 'thuật toán': 'programming_basic', 'OOP': 'programming_basic',
                    'hướng đối tượng': 'programming_basic',
                    'Black Box': 'tester', 'tester': 'tester', 'White Box': 'tester', 'Unit Test': 'tester',
                    'TestRail': 'tester', 'kiểm thử': 'tester',
                    'SVN': 'version_control', 'SCM': 'version_control', 'Git': 'version_control'}

languages = [' CHAIN ', ' ABAP ', 'Lingo', ' CPL', 'NPL', 'Xtend', ' Flex ', ' Io ', 'Erlang', 'Python', 'MSL', 'SAIL', ' Chef ', 'RPG', 'PHP',
             'XPath', 'Lava', ' Clojure', 'Mathematica', 'QPL', 'Oak', 'Objective-C', 'P#', 'css', ' Delphi', ' A+ ', 'XQuery',
             'CoffeeScript', ' SR ', 'ECMAScript', 'Nial', ' Red ', 'Mesa', 'LSL', 'T-SQL', 'E#', ' GAP ', 'Simula', 'Logo', ' Caml',
             'JavaScript', 'PeopleCode', ' UNITY ', ' Blue ', 'Span', 'Lucid', ' BeanShell', ' CSP', 'Scheme', 'Swift', 'TypeScript',
             ' Scala ', 'Scratch', 'Strand', 'XML', ' SAS', 'PortablE', 'JADE', ' C ', 'Processing', 'Pure', ' BASIC ', ' Bash', 'Pro*C',
             'ROOP', 'PL/SQL', 'Icon', ' Dart', ' Factor ', 'Java', 'LINC', ' Go ', ' TIE ', ' Cool', 'Kotlin', 'Rust', 'Opa', 'DYNAMO',
             ' Inform ', 'Mary', 'Ruby', 'YQL', 'Pike', ' rc ', ' html ', 'Oz', 'Groovy', 'PowerShell', ' CUDA', 'Hack', ' Self ',
             ' CFEngine', 'C#', 'SPS']

salary_patterns = ["lương(?:từ| )+ ((?:\d+|\.)+)", "((?:\d+|\.|-| )+(?:triệu| )+)đồng",
                   "(?:\d|\.|,)+.000.000", "(?:\d+| |-)+\d+ *(?:triệu|m)", "\$(?:\d+|\.)", "(?:\d+|\.)+ *(?:USD|\$)+",
                   "(?:\d|\.|,)+,000,000"]
