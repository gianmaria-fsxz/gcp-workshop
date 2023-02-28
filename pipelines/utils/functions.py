from datetime import datetime
   
def date_convert(date_to_convert):
     return abs((datetime.strptime(date_to_convert, "%Y-%m-%d") - datetime.today()).days)
