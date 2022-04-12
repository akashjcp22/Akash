from datetime import datetime, timedelta
import time
import requests
import pyinputplus as pyip
from playsound import playsound


print("**Please enter the state and district name as you see in Cowin Portal**")

header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"}

states = {
      "Andaman and Nicobar Islands": 1,
      "Andhra Pradesh": 2,
      "Arunachal Pradesh": 3,
      "Assam": 4,
      "Bihar": 5,
      "Chandigarh": 6,
      "Chhattisgarh": 7,
      "Dadra and Nagar Haveli": 8,
      "Daman and Diu": 37,
      "Delhi": 9,
      "Goa": 10,
      "Gujarat": 11,
      "Haryana": 12,
      "Himachal Pradesh": 13,
      "Jammu and Kashmir": 14,
      "Jharkhand": 15,
      "Karnataka": 16,
      "Kerala": 17,
      "Ladakh": 18,
      "Lakshadweep": 19,
      "Madhya Pradesh": 20,
      "Maharashtra": 21,
      "Manipur": 22,
      "Meghalaya": 23,
      "Mizoram": 24,
      "Nagaland": 25,
      "Odisha": 26,
      "Puducherry": 27,
      "Punjab": 28,
      "Rajasthan": 29,
      "Sikkim": 30,
      "Tamil Nadu": 31,
      "Telangana": 32,
      "Tripura": 33,
      "Uttar Pradesh": 34,
      "Uttarakhand": 35,
      "West Bengal": 36
    }

stateFlag = True
while stateFlag:
    inp_state = input("Enter state name: ")
    if inp_state in states.keys():
        stateFlag = False
    else:
        print("Enter state name as in portal")

district_url = "https://cdn-api.co-vin.in/api/v2/admin/location/districts/{0}".format(states[inp_state])

district_page_response = requests.get(district_url, headers=header)
district_page_json = district_page_response.json()

districts = list()
for district_dict in district_page_json["districts"]:
    districts.append(district_dict["district_name"])

print("Select your district from this list")
print(districts)

districtFlag = True
while districtFlag:
    inp_district = input("Enter district name: ")
    if inp_district in districts:
        districtFlag = False
    else:
        print("Enter district name as in the above list")

def getDistrictID(inp_distrct):
    for district_dict in district_page_json["districts"]:
        if district_dict["district_name"] == inp_district:
            return district_dict["district_id"]


district_id = getDistrictID(inp_district)

print("Choose the age as 18 and above or 45 and above")
age = int(pyip.inputChoice(['18', '45']))

print("Choose the dosage")
dose = int(pyip.inputChoice(['1', '2']))


today_date = datetime.today()
tmrw_date = today_date + timedelta(1)
tmrw_date_format = tmrw_date.strftime('%d-%m-%y')

final_url = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/findByDistrict?district_id={0}&date={1}".format(district_id, tmrw_date_format)


def availability():
    count = 0
    final_result = requests.get(final_url, headers=header)
    final_result_json = final_result.json()

    for centre in final_result_json["sessions"]:
        if((centre["available_capacity"] > 0) & (centre["min_age_limit"] <= age)):
            if(((dose == 1) & (centre["available_capacity_dose1"] > 0)) | ((dose == 2) & (centre["available_capacity_dose2"] > 0))):
                count += 1
                print("**********************************************************")
                print()
                print(centre["name"])
                print(centre["address"])
                print(centre["pincode"])
                if dose == 1:
                    print(centre["available_capacity_dose1"])
                else:
                    print(centre["available_capacity_dose2"])
                print()

    if(count >0):
        playsound(r"C:\Users\aj63\Downloads\message_notification.mp3") // Play a message notification sound when slots are available
        return True
    else:
        print("No slots are available now")
        return False



while True:

    if availability() == True:
        time.sleep(20)
        val = availability()
        if val is True:
            time.sleep(20)
        else:
            time.sleep(5)
    else:
        time.sleep(5)
        val = availability()
        if val is True:
            time.sleep(20)
        else:
            time.sleep(5)
