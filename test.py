import requests
from bs4 import BeautifulSoup

url = "https://www.topcv.vn/tim-viec-lam-software-engineering-cr257cb258?company_field=0&exp=0&page=2&salary=0&sort=up_top"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    "Referer": "https://www.topcv.vn/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Cookie": "_gcl_au=1.1.89424872.1746779155; _fbp=fb.1.1746779155183.614804561786376678; _clck=zkdpsy%7C2%7Cfvr%7C0%7C1955; _gid=GA1.2.607061707.1746779157; _tafp=539ccee6389642253ca45d60ea672c69; _taid=pMi9rzJFy3.1746779158557; popup-ebook-cv=1; g_state={"i_p":1746791380464,"i_l":1}; cf_clearance=zZXJWQ2irD908Qplf6odFsQ.qhEDmVbRuJVcDEHVYNY-1746787306-1.2.1.1-UYSqseDgYLGPJi3F7iVR5lfb4xQAZ1x.aMuWkyKv8Q4QlBaUiN_.TBwva0t9f_NbjCh6IF1aCEAdjbaUrvX0zVlV184saJ6dsWuZRPVa9PifAQn3H2pjoGRd7wIoi5GmIr7WBmz7DwzMPwl8qPUq3KfZ57OGebUGjHeiibq52YyK.1HI_KqW1wgaNe9NYDYmz5cDa5EOsXVEYc3wIQgJasypjbLPpuJuMC3oZvgFub6uiVKHeaiOuHZkwOfdlS.iWdB3sn.6HV6dKYW47eDWCEwsfgRtf8s4_NtnA7FLBx61YTBx0fy83iHkRlgsWYnsAsrVy9AEHfz7sa_f2GdBTaM8G7VvE3U.YcFeUBr8Q_I; _ga=GA1.2.2054830818.1746779153; _clsk=1tie1mo%7C1746787408363%7C5%7C0%7Ck.clarity.ms%2Fcollect; _ga_F385SHE0Y3=GS2.1.s1746783969$o2$g1$t1746787411$j53$l0$h0; XSRF-TOKEN=eyJpdiI6InAwV2dRa0J1ODlyYUZQRDFkSkhBUEE9PSIsInZhbHVlIjoiTWdYYmZJZTF4MGRaYXQ0STFRVEdDMzFDMjdBOGN5am9XbEJCNjZtOWlxODRlYjJrdlRpdDNFSnRJOStGRGdjSzh3azVZYXJtQ0QvRDlTU2t5UGU0dUhLSUtJL3R2UXowRHg2RkM3UnNSd0RvTllmdEdWVld2dzlSK0ozbi9qMHMiLCJtYWMiOiIxZDhjMjM2OTE0ZmU2YmFmNjE5ZjM2YmZhYzcyZWVjMTJkNjQ4YzBhMjc1NWNlOTllYzc1NjliZTc0NWU0NWI5IiwidGFnIjoiIn0%3D; topcv_session=eyJpdiI6IkpBenIyTmswaTA1TEpyZHBKWmpEOXc9PSIsInZhbHVlIjoiZW9iaGZRN00yT1piL1NLMDRCY2g2S2Q1K1NNTENqTHRkUWI5ZENHTHZCWGhqeGkvaWRrNVZWU3Q1Rm5QNmFva094TFU1bWl3N3RnYmtZT295S3RlRVEzbnFmaW1NZ3gzc1ZZUXgySGszVWg3cWttVDRSOGI0NVlzNTc5bHROclMiLCJtYWMiOiI5NTVlYWJiNTY4NzY2MDc4MzY1ODBmNmQzZjQ3ZDQwMTliYjg2MDYwZTExOThlMzk5YmViODgyNmVkYjE1MTZlIiwidGFnIjoiIn0%3D",  # Dán nguyên cookie của bạn
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    print("Trang được crawl thành công")
    # Tiếp tục xử lý HTML như tìm thẻ h3, a, v.v...
else:
    print("Lỗi khi truy cập trang:", response.status_code)
