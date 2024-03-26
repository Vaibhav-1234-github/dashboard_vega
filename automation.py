import pywhatkit
import time
from datetime import datetime 
import pyautogui 



msg = """
हरे कृष्णा प्रभुजी 🙏🙏
दंडवत प्रणाम

मैं HG अमोघ लीला प्रभुजी के SMA टीम से वैभव हूं।
प्रभुजी द्वारा व्यक्तिगत रूप से मान्यित एक लाइव टू गिव सेवा का अवसर है।
कृपया इस सेवा में भाग लें।
"""

numbers = [
"+916264096998",
"+917827634689",
"+918910188630",
"+917500897610",
"+919004290303",
"+917355677306",
"+916395063918",
"+919861478132",
"+919051169023",
"+919608081126",
"+919803776619",
"+917620615939",
"+919034975366",
"+916203955292",
"+917378791610",
"+918250247113",
"+919953824057",
"+918019152087",
"+918759394715",
"+917426065766",
"+919999426639",
"+918826658635",
"+918319552007",
"+919623891263",
"+918603919422",
"+919022335077",
"+917391072272",
"+919922142228",
"+917775047236",
"+918766893849",
"+918391802839",
"+917983980942",
"+919561654754",
"+917699393733",
"+919156973753",
"+916371811415",


"+918595782245",
"+918826658635",
"+919130169647",
"+917903089630",
"+918669090499",
"+918291789804",
"+916265268650",
"+918177849874",
"+918018211377",
"+919877341520",
"+918200264597"
]

#Time has to change variably for it to happen right 
wait_time = 30
my_obj  = datetime.now()

#getting the current hours and minutes
start_hour = my_obj.hour 
start_min = my_obj.minute  



for contact in numbers:
    start_min = start_min + 1    
    try:
        if start_min  > 59:
            start_min = start_min % 60 
            start_hour =  start_hour + 1 

        
        pywhatkit.sendwhatmsg(contact,msg,start_hour,start_min,wait_time=wait_time)
        pyautogui.click() 
        pyautogui.press('enter') 
        time.sleep(5)

    except Exception as e:
        print(e)




#limitations 
#opening a instance of app each time it is running , so as  many numbers are there as many instances are being opened 
#it is delivering  just 1 message each minute ,so kind of very slow for sending messages to a large number of people 




             
        


