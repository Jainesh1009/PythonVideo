from fastapi import FastAPI
import requests
import json
from Utils import *

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/getData/")
async def getData(data: dict):

    url = "https://epic.eris.co.in/getAccessTokenForUser?userID=" + \
        str(data['userId'])

    payload = ""

    headers = {'Authorization': 'Basic YWRtaW46TXlCQCQhQ0F1dGhQQFMk',
               'Cookie': 'ApplicationGatewayAffinity=ca0f629ac7201041c05c8cbabcc51ae0; ApplicationGatewayAffinityCORS=ca0f629ac7201041c05c8cbabcc51ae0; MONITOR=s%3ABLGK4D4dvGW8-_I-ux1fXGMxrXwAiS14.0x%2Fr2H2YTBxE4FawaiP5NfIe23dbJgYpT1ZV88tgquE'}

    response = requests.request("GET", url, headers=headers, data=payload)

    converted_response_to_json = json.loads(response.text)

    access_token = converted_response_to_json["access_token"]

    rowColumnKPIMapping = data['rowColumnKPIMapping']

    finalTrendData = []

    for item in rowColumnKPIMapping:
        j = await getTrendData(
            data['userId'],
            item['row'],
            item['columnKPIMapping'],
            data['referenceDate'],
            data['KPIList'],
            data['localFilters'],
            data['KPIWiseFilters'],
            data['granularity'],
            data['NoOfPoints'],
            data['StartDate'],
            data['componentSettings'],
            data['componentSettings']['requiredDimensions'],
            access_token,
            headers
        )
        finalTrendData = [*finalTrendData, *j]

    ListKPI = await getKPIname(rowColumnKPIMapping)

    if (len(data['componentSettings']['requiredDimensions']) == 6):

        kpiValueDatad = await PrepareFinalDataDownloadForSixDimension(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    elif (len(data['componentSettings']['requiredDimensions']) == 7):

        kpiValueDatad = await PrepareFinalDataDownloadForSevenDimension(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    elif (len(data['componentSettings']['requiredDimensions']) == 8):

        kpiValueDatad = await PrepareFinalDataDownloadForEightDimension(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    elif (len(data['componentSettings']['requiredDimensions']) == 9):

        kpiValueDatad = await PrepareFinalDataDownloadForNineDimension(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    elif (len(data['componentSettings']['requiredDimensions']) == 5):

        kpiValueDatad = await PrepareFinalDataDownloadForFiveDimension(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    else:
        kpiValueDatad = await PrepareFinalDataDownload(
            finalTrendData,
            ListKPI,
            data['componentSettings']['requiredDimensions']
        )

    return {"FinalData": kpiValueDatad, "baseData": finalTrendData}


async def getTrendData(
    userId,
    Row,
    ColumnKPIMapping,
    referenceDate,
    KPIList,
    localFilters,
    KPIWiseFilters,
    selectedGranularity,
    NoOfPoints,
    StartDate,
    componentSettings,
    requiredDimensions,
    access_token,
    headers
):

    urlForTrend = "https://epic.eris.co.in/apis/monitorBackend/getKPITrendForMainKPI?access_token="+access_token

    finalData = []
    for item in ColumnKPIMapping:

        filters = await getFilters(
            access_token,
            headers,
            userId,
            KPIList,
            item,
            localFilters,
            KPIWiseFilters
        )

        payloadForBodyDataForTrend = {
            "kpiId": KPIList[item["kpi"]]["mainkpiid"],
            "granularity": selectedGranularity,
            "toDate": referenceDate,
            "filterDimension": filters["filterDimensions"],
            "filterDimensionValue": filters["filterDimensionValues"],
            "dimensionList": requiredDimensions,
            "fromDate": StartDate,
            "numPoints": None,
            "tag": None,
            "isMaxDateCheckRequired": False
        }

        responseTrendData = requests.request(
            "GET", urlForTrend, headers=headers, data=payloadForBodyDataForTrend)

        converted_res_Trend_Data_to_json = responseTrendData.text

        converted_res_Trend_Data_to_json = json.loads(
            converted_res_Trend_Data_to_json)

        for i in converted_res_Trend_Data_to_json:
            i["KPI"] = Row
            i[Row] = i["value"]

        finalData = [*finalData, *converted_res_Trend_Data_to_json]

    return finalData


async def getFilters(access_token, headers, userId, KPIList, item, localFilters, KPIWiseFilters):
    urlForConfig = "https://epic.eris.co.in/apis/monitorBackend/getUserMainKPIConfig?access_token="+access_token

    newLocalFilters = localFilters

    payloadForConfig = {
        "kpiId": KPIList[item["kpi"]]["mainkpiid"],
        "userId": userId
    }

    responseConfig = requests.request(
        "GET", urlForConfig, headers=headers, data=payloadForConfig)

    converted_res_Config_to_json = responseConfig.text

    converted_res_Config_to_json = json.loads(
        converted_res_Config_to_json)

    filters = await getKPIFilters(KPIList, item, newLocalFilters, KPIWiseFilters, converted_res_Config_to_json)

    return filters


def mergeLocalFilters(localFilters, filterDimension, filterDimensionValue):
    mergeFilter = {*localFilters}
    if len(filterDimension) == 0:
        return localFilters
    for d in range(len(filterDimension)):
        if not mergeFilter[filterDimension[d]]:
            mergeFilter[filterDimension[d]] = filterDimensionValue[d]
        else:
            mergeFilter[filterDimension[d]] = list(
                set(mergeFilter[filterDimension[d]] + filterDimensionValue[d]))
    return mergeFilter


async def getKPIFilters(KPIList, item, localFilters, KPIWiseFilters, KPIConfig):
    tempFilterObj = localFilters
    filterObj = {}
    for dim in tempFilterObj:
        tempObj = list(filter(lambda d: d["key"] == dim or d["matchingKey"]
                       == dim, KPIConfig["dimensionConfig"]["dimensions"]))
        if len(tempObj) > 0:
            filterObj[tempObj[0]["key"]] = tempFilterObj[dim]

    return mergeAllFilters(filterObj, KPIWiseFilters, KPIList[item["kpi"]]["mainkpiid"])


def mergeAllFilters(localFilters, globalFilters, mainKpiId):
    mergeDimensions = {}

    globalDimensions = globalFilters[mainKpiId]["filterDimensions"] if globalFilters and globalFilters[
        mainKpiId] and globalFilters[mainKpiId]["filterDimensions"] else []
    globalDimensionValues = globalFilters[mainKpiId]["filterDimensionValues"] if globalFilters and globalFilters[
        mainKpiId] and globalFilters[mainKpiId]["filterDimensionValues"] else []

    for mainFilter, mainFilterIndex in enumerate(globalDimensions):
        mergeDimensions[mainFilter] = globalDimensionValues[mainFilterIndex]

    for d in localFilters:
        if d in mergeDimensions:
            mergeDimensions[d] = list(
                filter(lambda e: e in localFilters[d], mergeDimensions[d]))
        else:
            mergeDimensions[d] = localFilters[d]

    return {
        "filterDimensions": list(mergeDimensions.keys()),
        "filterDimensionValues": list(mergeDimensions.values())
    }
