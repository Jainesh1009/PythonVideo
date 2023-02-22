async def PrepareFinalDataDownload(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        finalObj1 = {"KPI": i}

                        for obj in dataTemp3:
                            title = obj['groupByDate']
                            finalObj1[title] = obj[i]

                            finalObj1[dimensions[0]] = obj[dimensions[0]]
                            finalObj1[dimensions[1]] = obj[dimensions[1]]
                            finalObj1[dimensions[2]] = obj[dimensions[2]]
                            finalObj1[dimensions[3]] = obj[dimensions[3]]

                        dataprepare = [*dataprepare, finalObj1]

    return dataprepare


async def PrepareFinalDataDownloadForSixDimension(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        UniqueDimension5 = await getUniqueDimensions(dataTemp3, dimensions[4])

                        for item5 in UniqueDimension5:
                            dataTemp4 = list(
                                filter(lambda x: x[dimensions[4]] == item5, dataTemp3))

                            UniqueDimension6 = await getUniqueDimensions(dataTemp4, dimensions[5])

                            for item6 in UniqueDimension6:
                                dataTemp5 = list(
                                    filter(lambda x: x[dimensions[5]] == item6, dataTemp4))

                                finalObj1 = {"KPI": i}

                                for obj in dataTemp5:
                                    title = obj['groupByDate']
                                    finalObj1[title] = obj[i]

                                    finalObj1[dimensions[0]
                                              ] = obj[dimensions[0]]
                                    finalObj1[dimensions[1]
                                              ] = obj[dimensions[1]]
                                    finalObj1[dimensions[2]
                                              ] = obj[dimensions[2]]
                                    finalObj1[dimensions[3]
                                              ] = obj[dimensions[3]]
                                    finalObj1[dimensions[4]
                                              ] = obj[dimensions[4]]
                                    finalObj1[dimensions[5]
                                              ] = obj[dimensions[5]]

                                dataprepare = [*dataprepare, finalObj1]

    return dataprepare


async def PrepareFinalDataDownloadForSevenDimension(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        UniqueDimension5 = await getUniqueDimensions(dataTemp3, dimensions[4])

                        for item5 in UniqueDimension5:
                            dataTemp4 = list(
                                filter(lambda x: x[dimensions[4]] == item5, dataTemp3))

                            UniqueDimension6 = await getUniqueDimensions(dataTemp4, dimensions[5])

                            for item6 in UniqueDimension6:
                                dataTemp5 = list(
                                    filter(lambda x: x[dimensions[5]] == item6, dataTemp4))

                                UniqueDimension7 = await getUniqueDimensions(dataTemp5, dimensions[6])

                                for item7 in UniqueDimension7:
                                    dataTemp6 = list(
                                        filter(lambda x: x[dimensions[6]] == item7, dataTemp5))

                                    finalObj1 = {"KPI": i}

                                    for obj in dataTemp6:
                                        title = obj['groupByDate']
                                        finalObj1[title] = obj[i]

                                        finalObj1[dimensions[0]
                                                  ] = obj[dimensions[0]]
                                        finalObj1[dimensions[1]
                                                  ] = obj[dimensions[1]]
                                        finalObj1[dimensions[2]
                                                  ] = obj[dimensions[2]]
                                        finalObj1[dimensions[3]
                                                  ] = obj[dimensions[3]]
                                        finalObj1[dimensions[4]
                                                  ] = obj[dimensions[4]]
                                        finalObj1[dimensions[5]
                                                  ] = obj[dimensions[5]]
                                        finalObj1[dimensions[6]
                                                  ] = obj[dimensions[6]]

                                    dataprepare = [*dataprepare, finalObj1]

    return dataprepare


async def PrepareFinalDataDownloadForEightDimension(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        UniqueDimension5 = await getUniqueDimensions(dataTemp3, dimensions[4])

                        for item5 in UniqueDimension5:
                            dataTemp4 = list(
                                filter(lambda x: x[dimensions[4]] == item5, dataTemp3))

                            UniqueDimension6 = await getUniqueDimensions(dataTemp4, dimensions[5])

                            for item6 in UniqueDimension6:
                                dataTemp5 = list(
                                    filter(lambda x: x[dimensions[5]] == item6, dataTemp4))

                                UniqueDimension7 = await getUniqueDimensions(dataTemp5, dimensions[6])

                                for item7 in UniqueDimension7:
                                    dataTemp6 = list(
                                        filter(lambda x: x[dimensions[6]] == item7, dataTemp5))

                                    UniqueDimension8 = await getUniqueDimensions(dataTemp6, dimensions[7])

                                    for item8 in UniqueDimension8:
                                        dataTemp7 = list(
                                            filter(lambda x: x[dimensions[7]] == item8, dataTemp6))

                                        finalObj1 = {"KPI": i}

                                        for obj in dataTemp7:
                                            title = obj['groupByDate']
                                            finalObj1[title] = obj[i]

                                            finalObj1[dimensions[0]
                                                      ] = obj[dimensions[0]]
                                            finalObj1[dimensions[1]
                                                      ] = obj[dimensions[1]]
                                            finalObj1[dimensions[2]
                                                      ] = obj[dimensions[2]]
                                            finalObj1[dimensions[3]
                                                      ] = obj[dimensions[3]]
                                            finalObj1[dimensions[4]
                                                      ] = obj[dimensions[4]]
                                            finalObj1[dimensions[5]
                                                      ] = obj[dimensions[5]]
                                            finalObj1[dimensions[6]
                                                      ] = obj[dimensions[6]]
                                            finalObj1[dimensions[7]
                                                      ] = obj[dimensions[7]]

                                        dataprepare = [*dataprepare, finalObj1]

    return dataprepare


async def PrepareFinalDataDownloadForNineDimension(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        UniqueDimension5 = await getUniqueDimensions(dataTemp3, dimensions[4])

                        for item5 in UniqueDimension5:
                            dataTemp4 = list(
                                filter(lambda x: x[dimensions[4]] == item5, dataTemp3))

                            UniqueDimension6 = await getUniqueDimensions(dataTemp4, dimensions[5])

                            for item6 in UniqueDimension6:
                                dataTemp5 = list(
                                    filter(lambda x: x[dimensions[5]] == item6, dataTemp4))

                                UniqueDimension7 = await getUniqueDimensions(dataTemp5, dimensions[6])

                                for item7 in UniqueDimension7:
                                    dataTemp6 = list(
                                        filter(lambda x: x[dimensions[6]] == item7, dataTemp5))

                                    UniqueDimension8 = await getUniqueDimensions(dataTemp6, dimensions[7])

                                    for item8 in UniqueDimension8:
                                        dataTemp7 = list(
                                            filter(lambda x: x[dimensions[7]] == item8, dataTemp6))

                                        UniqueDimension9 = await getUniqueDimensions(dataTemp7, dimensions[8])

                                        for item9 in UniqueDimension9:
                                            dataTemp8 = list(
                                                filter(lambda x: x[dimensions[8]] == item9, dataTemp7))

                                            finalObj1 = {"KPI": i}

                                            for obj in dataTemp8:
                                                title = obj['groupByDate']
                                                finalObj1[title] = obj[i]

                                                finalObj1[dimensions[0]
                                                          ] = obj[dimensions[0]]
                                                finalObj1[dimensions[1]
                                                          ] = obj[dimensions[1]]
                                                finalObj1[dimensions[2]
                                                          ] = obj[dimensions[2]]
                                                finalObj1[dimensions[3]
                                                          ] = obj[dimensions[3]]
                                                finalObj1[dimensions[4]
                                                          ] = obj[dimensions[4]]
                                                finalObj1[dimensions[5]
                                                          ] = obj[dimensions[5]]
                                                finalObj1[dimensions[6]
                                                          ] = obj[dimensions[6]]
                                                finalObj1[dimensions[7]
                                                          ] = obj[dimensions[7]]
                                                finalObj1[dimensions[8]
                                                          ] = obj[dimensions[8]]

                                            dataprepare = [
                                                *dataprepare, finalObj1]

    return dataprepare

async def PrepareFinalDataDownloadForFiveDimension(
    kpiValueDatad,
    ListKPI,
    requiredDimensions
):
    dataprepare = []
    dimensions = requiredDimensions
    for i in ListKPI:
        data = list(filter(lambda x: x['KPI'] == i, kpiValueDatad))

        UniqueDimension1 = await getUniqueDimensions(data, dimensions[0])

        for item1 in UniqueDimension1:
            dataTemp = list(filter(lambda x: x[dimensions[0]] == item1, data))

            UniqueDimension2 = await getUniqueDimensions(dataTemp, dimensions[1])

            for item2 in UniqueDimension2:
                dataTemp1 = list(
                    filter(lambda x: x[dimensions[1]] == item2, dataTemp))

                UniqueDimension3 = await getUniqueDimensions(dataTemp1, dimensions[2])

                for item3 in UniqueDimension3:
                    dataTemp2 = list(
                        filter(lambda x: x[dimensions[2]] == item3, dataTemp1))

                    UniqueDimension4 = await getUniqueDimensions(dataTemp2, dimensions[3])

                    for item4 in UniqueDimension4:
                        dataTemp3 = list(
                            filter(lambda x: x[dimensions[3]] == item4, dataTemp2))

                        UniqueDimension5 = await getUniqueDimensions(dataTemp3, dimensions[4])

                        for item5 in UniqueDimension5:
                            dataTemp4 = list(
                                filter(lambda x: x[dimensions[4]] == item5, dataTemp3))

                            finalObj1 = {"KPI": i}

                            for obj in dataTemp4:
                                title = obj['groupByDate']
                                finalObj1[title] = obj[i]

                                finalObj1[dimensions[0]
                                          ] = obj[dimensions[0]]
                                finalObj1[dimensions[1]
                                          ] = obj[dimensions[1]]
                                finalObj1[dimensions[2]
                                          ] = obj[dimensions[2]]
                                finalObj1[dimensions[3]
                                          ] = obj[dimensions[3]]
                                finalObj1[dimensions[4]
                                          ] = obj[dimensions[4]]

                            dataprepare = [*dataprepare, finalObj1]

    return dataprepare

async def getKPIname(rowColumnKPIMapping):
    KPIname = []
    for j in rowColumnKPIMapping:
        KPIname.append(j['row'])
    return KPIname


async def getUniqueDimensions(data, dimension):
    DistinctDimension_Name = []
    for item in data:
        DistinctDimension_Name.append(item[dimension])
    uniqDimension_Name = [*set(DistinctDimension_Name)]
    return uniqDimension_Name