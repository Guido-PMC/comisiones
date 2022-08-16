import json
import os
import requests
import time
import random
import datetime
from datetime import date
from datetime import datetime
import gspread
from gspread_dataframe import *
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import cronitor
import schedule


credenciales = os.environ['CREDS']
tuple_months = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
mesFin = datetime.now().month-1
current_month = tuple_months[mesFin]


class client:
    def __init__(self, id, status, startDate, minerType,commissionType, commission, consumption, name, wallet, totalmined, paymentPending, cashPaymentPending):
        self.id = id
        self.status = status
        self.startDate = startDate
        self.minerType = minerType
        self.commissionType = commissionType
        self.commission = commission
        self.consumption = consumption
        self.name = name
        self.wallet = wallet
        self.totalMined = totalmined
        self.paymentPending = paymentPending
        self.cashPaymentPending = cashPaymentPending

    def showAll(self):
        return self.id,self.name, self.commission, self.totalMined, self.minerType,self.paymentPending,self.cashPaymentPending,0

def createSheet(documento,name,row,col):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    sheet = client.open(documento)
    sheet.add_worksheet(title=name, rows=row, cols=col)


def updateSheetsWithDataframe(documento,hoja,dataframe):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    sheet = client.open(documento)
    sheet_instance = sheet.worksheet(hoja)
    sheet_instance.clear()
    sheet_instance.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

def formatSheets(documento,hoja,rango,flag):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    sheet = client.open(documento)
    sheet_instance = sheet.worksheet(hoja)
    if (flag == "titulo"):
        sheet_instance.format(rango, {
        "backgroundColor": {
          "red": 0.0,
          "green": 0.0,
          "blue": 0.0
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
          "foregroundColor": {
            "red": 1.0,
            "green": 1.0,
            "blue": 1.0
          },
          "fontSize": 12,
          "bold": True
        }
    })
    if (flag=="centro"):
        sheet_instance.format(rango, {
        "horizontalAlignment": "CENTER"
    })
    if (flag=="rojo"):
        sheet_instance.format(rango, {
        "backgroundColor": {
          "red": 1,
          "green": 0.2,
          "blue": 0.3
        }
})
def getSheetsDataFrame(sheet, worksheet):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    work_sheet = client.open(sheet)
    sheet_instance = work_sheet.worksheet(worksheet)
    records_data = sheet_instance.get_all_records()
    return (pd.DataFrame.from_dict(records_data))

def getComisionEthereum(id, wallet, comision, inicioPeriodoFacturacion, finPeriodoFacturacion, gwei_to_eth):
    #sanitizo Comision
    if (id < 10):
        id = "0"+str(id)

    try:
        comision = comision[:-1]
        comision = comision.replace("," , ".")
        URL = "https://api.ethermine.org/miner/"
        URL = URL[:32] + wallet + '/payouts'
        r = requests.get(url = URL)
        data = r.json()
        tempEthWallet = 0
        totalcomisiones = 0
        clientComision = 0
        count = 0
        for x in data["data"]:
            if x["paidOn"] > inicioPeriodoFacturacion and x["paidOn"] < finPeriodoFacturacion:
                monto = x["amount"] / gwei_to_eth
                tempEthWallet = tempEthWallet + monto

        totalminado = str(tempEthWallet)#[:7]
        totalminado = totalminado[:9]
        clientComision = (float(totalminado) * float(comision) /100)
        clientComision = str(clientComision)[:9]+str(id)
        clientComision = float(clientComision)
    except Exception as e:
        print (e)
        return 0,0
    return clientComision, float(totalminado)

def getUnixTimeStamp(mesInicio,mesFin,diaInicio,diaFin):
    ano = int(datetime.now().year)
    inicioPeriodoFacturacion = date(int(ano),int(mesInicio),int(diaInicio))
    finPeriodoFacturacion = date(int(ano),int(mesFin),int(diaFin))
    unixInicioPeriodoFacturacion = time.mktime(inicioPeriodoFacturacion.timetuple())
    unixFinPeriodoFacturacion = time.mktime(finPeriodoFacturacion.timetuple())
    return unixInicioPeriodoFacturacion, unixFinPeriodoFacturacion

def downloadSheet(spreadsheet_name):
    scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
    ]
    creds=ServiceAccountCredentials.from_json_keyfile_name(credenciales,scope)
    client=gspread.authorize(creds)
    spreadsheet = client.open(spreadsheet_name)
    url = 'https://docs.google.com/spreadsheets/export?format=pdf&id=' + spreadsheet.id
    headers = {'Authorization': 'Bearer ' + creds.create_delegated("").get_access_token().access_token}
    res = requests.get(url, headers=headers)
    with open(spreadsheet_name + ".pdf", 'wb') as f:
        f.write(res.content)

def telegram_message(message):
    headers_telegram = {"Content-Type": "application/x-www-form-urlencoded"}
    endpoint_telegram = "https://api.telegram.org/bot1956376371:AAFgQ8zc6HLwRReXnzdfN7csz_-iEl8E1oY/sendMessage"
    mensaje_telegram = {'chat_id': '-634167024', 'text': 'Problemas en RIG'}
    mensaje_telegram["text"] = message
    response = requests.post(endpoint_telegram, headers=headers_telegram, data=mensaje_telegram).json()
    if (response["ok"] == False):
        print("Voy a esperar xq se bloquio telegram")
        time.sleep(response["parameters"]["retry_after"]+5)
        response = requests.post(endpoint_telegram, headers=headers_telegram, data=mensaje_telegram).json()
    return response

def shouldIRun():
    telegram_message(f"Hoy es: {datetime.now()}")

    actualDay = datetime.now().day
    diaInicio, diaFin = getSheetsDataFrame("Cobros - Autom 2.0","INFORMACION")["Ciclo"][0].split(" - ")
    if(int(actualDay) == int(diaInicio)+1):
        return True
        print("toca Correr automatismo")
    else:
        print("No toca correr automatismo")
        return False



def job():
    if(shouldIRun()):
        telegram_message("Corriendo SCRIPT de Comisiones")
        #VARIABLES
        mesInicio = (datetime.now().month - 1)
        mesFin = datetime.now().month
        diaInicio, diaFin = getSheetsDataFrame("Cobros - Autom 2.0","INFORMACION")["Ciclo"][0].split(" - ")
        gwei_to_eth = 1000000000000000000
        costPerKwh = float(getSheetsDataFrame("Cobros - Autom 2.0","INFORMACION")["Costo kWh"][0].replace("$",""))
        startDate, endDate = getUnixTimeStamp(mesInicio,mesFin,diaInicio,diaFin)
        clientsDictionary = getSheetsDataFrame("Cobros - Autom 2.0","Clientes Housing").to_dict()
        clientsObjList = []

        for i in range (0, len(clientsDictionary["id"])):
            if (clientsDictionary["status"][i]=="TRUE"):
                cashPaymentPending = float(clientsDictionary["consumo"][i]) * 24 * 30 / 1000 * costPerKwh
                if(clientsDictionary["tipo minero"][i]=="ethereum"):
                    paymentPending, totalMined = getComisionEthereum(clientsDictionary["id"][i], clientsDictionary["wallet"][i], clientsDictionary["comision"][i], startDate, endDate,gwei_to_eth)
                elif(clientsDictionary["tipo minero"][i]=="bitcoin"):
                    paymentPending, totalMined = 0,0
                clientObj = client(clientsDictionary["id"][i],clientsDictionary["status"][i],clientsDictionary["fecha inicio"][i],clientsDictionary["tipo minero"][i],clientsDictionary["tipo comision"][i],clientsDictionary["comision"][i],clientsDictionary["consumo"][i],clientsDictionary["nombre"][i],clientsDictionary["wallet"][i],totalMined,paymentPending,cashPaymentPending)
                clientsObjList.append(clientObj)

        dfClients = pd.DataFrame(columns=['id', 'nombre', 'comision','total minado', 'moneda', 'mes cripto', 'ars', 'usdt'])
        for x in clientsObjList:
            dfClients.loc[len(dfClients)] = (x.showAll())
            if x.minerType == "ethereum":
                telegram_message(f"Cliente: {(x.name)}\nMoneda: Ethereum \nWallet: {(x.wallet)}\nComision: {(x.commission)}\nPeriodo Calculado: {(diaInicio)}/{(mesInicio)} - {(diaFin)}/{(mesFin)}\nTotal Minado por Wallet: {(x.totalMined)}\nA cobrar por PMC: *{(x.paymentPending)}*\n\n*Por favor enviar exactamente: {str(float(x.paymentPending))[:11]} a la siguiente direccion RED BEP20* _(comision de Binance no sumada, por favor agregar antes de transferir)_ \n\nWallet PMC: 0x34fa7b1abfd6e397de3c39934635fedb925eea4d")
            elif x.minerType == "bitcoin":
                telegram_message(f"Cliente: {(x.name)}\nMoneda: Bitcoin \nWallet: {(x.wallet)}\nComision: {(x.commission)}\nPeriodo Calculado: {(diaInicio)}/{(mesInicio)} - {(diaFin)}/{(mesFin)}\nTotal Minado por Wallet: {(x.totalMined)}\nA cobrar por PMC ARS: *{(x.cashPaymentPending)}*\nA cobrar por PMC Cripto: *{(x.paymentPending)}*\n\n*Por favor enviar exactamente: {str(float(x.paymentPending))[:11]} a la siguiente direccion RED BEP20* _(comision de Binance no sumada, por favor agregar antes de transferir)_ \n\n*Por favor enviar exactamente: {x.cashPaymentPending} al CBU / ALIAS: 20396565154.LEMON*  \n\nWallet PMC: 0x34fa7b1abfd6e397de3c39934635fedb925eea4d")

        print(dfClients)
        try:
            createSheet("Cobros - Autom 2.0",current_month,len(clientsDictionary["id"]),len(dfClients.columns))
        except Exception as e:
            print(e)
            updateSheetsWithDataframe("Cobros - Autom 2.0",current_month,dfClients)
            formatSheets("Cobros - Autom 2.0",current_month,"A1:H1","titulo")#TITULO
            formatSheets("Cobros - Autom 2.0",current_month,"A1:H"+str(len(clientsDictionary["id"])),"centro")
            formatSheets("Cobros - Autom 2.0",current_month,"F2:F"+str(len(clientsDictionary["id"])-3),"rojo")
        downloadSheet('Cobros - Autom 2.0')


runHour = getSheetsDataFrame("Cobros - Autom 2.0","INFORMACION")["Horario Corrida"][0]
print(runHour)
schedule.every().day.at(runHour).do(job)
while True:
    newRunHour = getSheetsDataFrame("Cobros - Autom 2.0","INFORMACION")["Horario Corrida"][0]
    if runHour != newRunHour:
        telegram_message(f"Automatismo comisiones de Housing - Horario del script cambiado a {newRunHour}")
        exit()
    schedule.run_pending()
    time.sleep(10)
