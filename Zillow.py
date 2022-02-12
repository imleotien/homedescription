import requests
from bs4 import BeautifulSoup
from urllib import parse
import datetime
import hashlib
import pandas as pd
import time
import numpy as np

class Zillow():
    """
    TODOS:
    1. after each request increment self.id by 1 - DONE
    2. use listResults for cat1 and cat2, using pagenation to get all the data - need to do cat2
    3. save to csv - DONE
    """
    userAgents = [
        "Mozilla/5.0 (Windows; U; Windows CE) AppleWebKit/531.30.5 (KHTML, like Gecko) Version/5.0 Safari/531.30.5",
        "Mozilla/5.0 (compatible; MSIE 5.0; Windows NT 6.2; Trident/4.0)",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1) AppleWebKit/533.3.7 (KHTML, like Gecko) Version/4.1 Safari/533.3.7",
        "Opera/9.91 (Windows 98; en-US) Presto/2.12.236 Version/10.00",
        "Opera/8.65 (X11; Linux i686; sl-SI) Presto/2.10.291 Version/12.00",
        "Opera/9.91 (Windows NT 5.1; sl-SI) Presto/2.10.196 Version/10.00",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5351 (KHTML, like Gecko) Chrome/39.0.839.0 Mobile Safari/5351",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows 98; Trident/5.1)"    
    ]
    hashColumns = [
        "zpid",
        "id",
        "providerListingId",
        "imgSrc",
        "hasImage",
        "detailUrl",
        "statusType",
        "statusText",
        "countryCurrency",
        "price",
        "unformattedPrice",
        "address",
        "addressStreet",
        "addressCity",
        "addressState",
        "addressZipcode",
        "isUndisclosedAddress",
        "beds",
        "baths",
        "area",
        "isZillowOwned",
        "badgeInfo",
        "isSaved",
        "isUserClaimingOwner",
        "isUserConfirmedClaim",
        "pgapt",
        "sgapt",
        "zestimate",
        "shouldShowZestimateAsPrice",
        "has3DModel",
        "hasVideo",
        "isHomeRec",
        "info2String",
        "info3String",
        "brokerName",
        "hasAdditionalAttributions",
        "isFeaturedListing",
        "availabilityDate",
        "list",
        "relaxed",
        "latLong_latitude",
        "latLong_longitude",
        "variableData_type",
        "variableData_text",
        "variableData_data_isFresh",
        "hdpData_homeInfo_zpid",
        "hdpData_homeInfo_streetAddress",
        "hdpData_homeInfo_zipcode",
        "hdpData_homeInfo_city",
        "hdpData_homeInfo_state",
        "hdpData_homeInfo_latitude",
        "hdpData_homeInfo_longitude",
        "hdpData_homeInfo_price",
        "hdpData_homeInfo_bathrooms",
        "hdpData_homeInfo_bedrooms",
        "hdpData_homeInfo_livingArea",
        "hdpData_homeInfo_homeType",
        "hdpData_homeInfo_homeStatus",
        "hdpData_homeInfo_imageLink",
        "hdpData_homeInfo_daysOnZillow",
        "hdpData_homeInfo_isFeatured",
        "hdpData_homeInfo_shouldHighlight",
        "hdpData_homeInfo_brokerId",
        "hdpData_homeInfo_listing_sub_type_is_FSBA",
        "hdpData_homeInfo_isUnmappable",
        "hdpData_homeInfo_mediumImageLink",
        "hdpData_homeInfo_isPreforeclosureAuction",
        "hdpData_homeInfo_homeStatusForHDP",
        "hdpData_homeInfo_priceForHDP",
        "hdpData_homeInfo_timeOnZillow",
        "hdpData_homeInfo_hiResImageLink",
        "hdpData_homeInfo_watchImageLink",
        "hdpData_homeInfo_tvImageLink",
        "hdpData_homeInfo_tvCollectionImageLink",
        "hdpData_homeInfo_tvHighResImageLink",
        "hdpData_homeInfo_desktopWebHdpImageLink",
        "hdpData_homeInfo_isNonOwnerOccupied",
        "hdpData_homeInfo_isPremierBuilder",
        "hdpData_homeInfo_isZillowOwned",
        "hdpData_homeInfo_currency",
        "hdpData_homeInfo_country",
        "hdpData_homeInfo_taxAssessedValue",
        "hdpData_homeInfo_lotAreaValue",
        "hdpData_homeInfo_lotAreaUnit",
        "hdpData_homeInfo_zestimate",
        "hdpData_homeInfo_rentZestimate",
        "hasOpenHouse",
        "openHouseStartDate",
        "openHouseEndDate",
        "openHouseDescription",
        "hdpData_homeInfo_listing_sub_type_is_openHouse",
        "hdpData_homeInfo_openHouse",
        "hdpData_homeInfo_open_house_info_open_house_showing",
        "hdpData_homeInfo_dateSold",
        "hdpData_homeInfo_unit",
        "hdpData_homeInfo_listing_sub_type_is_newHome",
        "hdpData_homeInfo_newConstructionType",
        "builderName",
        "hdpData_homeInfo_group_type",
        "hdpData_homeInfo_grouping_name",
        "hdpData_homeInfo_priceSuffix",
        "hdpData_homeInfo_providerListingID",
        "hdpData_homeInfo_datePriceChanged",
        "hdpData_homeInfo_priceChange"
    ]
    fullColumns = hashColumns + ["LoadDate"]
    fileName = "../server/zillow.csv"
    
    def __init__(self, city, state):
        session = requests.session()
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.8',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows; U; Windows CE) AppleWebKit/531.30.5 (KHTML, like Gecko) Version/5.0 Safari/531.30.5'
        }
        self.city = city
        self.state = state
        session.headers = headers
        resp = session.get(f"https://www.zillow.com/homes/{city},-{state}_rb")
        self.session = session
        self.id = 1
        self.oldHash = set(pd.read_csv(self.fileName)["hash_index"])
        self._mapBound(self.session.cookies["search"])

    def _mapBound(self, searchCookie):
        data = parse.unquote(searchCookie).split("&")[1].split("%2C")
        d = {}
        d["north"] = float(data[0][5:])
        d["east"] = float(data[1])
        d["south"] = float(data[2])
        westRegion = data[3].split("\t")
        d["west"] = float(westRegion[0])
        d["regionID"] = int(westRegion[2])
        self.mapBoundData = d 


    # problem, can only get the top 500
    def getForSellQuery(self, currentPage:int):
        
        searchQueryState = parse.quote(str({
            "pagination":{"currentPage":currentPage},
            "usersSearchTerm":f"{self.city.replace('-', ' ')}, {self.state}",
            "mapBounds":{
                "west":self.mapBoundData["west"],
                "east":self.mapBoundData["east"],
                "south":self.mapBoundData["south"],
                "north":self.mapBoundData["north"]
            },
            "regionSelection":[{"regionId":self.mapBoundData["regionID"],"regionType":6}],
            "isMapVisible":True,
            "filterState":{"isAllHomes":{"value":True}},
            "isListVisible":True,
            "mapZoom":9}).replace(" ", "").replace("'", "\"")) # handle spacing w/ replaces
        wants = "{%22cat1%22:[%22listResults%22]}" # cat1 = Agent Listing, cat2 = Other Listing, listResults = Cards, mapResults = Map items (500 max)
        requestID = self.id
        url = f"https://www.zillow.com/search/GetSearchPageState.htm?searchQueryState={searchQueryState}&wants={wants}&requestID={requestID}"
        return url
    
    def getForRentQuery(self, currentPage:int):
        searchQueryState = parse.quote(str({
            "pagination":{"currentPage":1},
            "usersSearchTerm":f"{self.city.replace('-', ' ')}, {self.state}",
            "mapBounds":{
                "west":self.mapBoundData["west"],
                "east":self.mapBoundData["east"],
                "south":self.mapBoundData["south"],
                "north":self.mapBoundData["north"]},
            "regionSelection":[{"regionId":self.mapBoundData["regionID"],"regionType":6}],
            "isMapVisible":True,
            "filterState":{
                    "isForSaleForeclosure":{"value":False},
                    "isAllHomes":{"value":True},
                    "isAuction":{"value":False},
                    "isNewConstruction":{"value":False},
                    "isForRent":{"value":True},
                    "isForSaleByOwner":{"value":False},
                    "isComingSoon":{"value":False},
                    "isForSaleByAgent":{"value":False}
                },
            "isListVisible":True,
            "mapZoom":9
        }).replace(" ", "").replace("'", "\""))

        return url


    def sendInitRequest(self, _type):
        """
        assigns a bunch of values only possible with a response from Zillow
        type: sell, rent, sold
        """
        if _type == "sell":
            
            resp = self.session.get(self.getForSellQuery(1))
        elif _type == "rent":
            resp = self.session.get(self.getForSellQuery(1))

        data = resp.json()
        self.totalPages = data["categoryTotals"]["cat1"]["totalResultCount"] // 40 + 1 + 2
        self.id += 1
        df = pd.DataFrame(columns=self.hashColumns)
        tempDF = pd.json_normalize(data["cat1"]["searchResults"]["listResults"], sep="_")
        for colName in tempDF:
            df[colName] = tempDF[colName]

        df["hash_index"] = [hashlib.md5(str(tuple(i)).encode()).hexdigest() for i in df[self.hashColumns].itertuples(index=False)]
        df["LoadDate"] = datetime.datetime.now()
        df.set_index("hash_index", inplace=True)
        df.loc[set(df.index) - self.oldHash, self.fullColumns].to_csv(self.fileName, mode="a", header=False, index_label="hash_index")
        print(self.totalPages)
        return data

    def sendRequest(self, _type):
        for i in range(2, self.totalPages + 1):
            if self.id % 10 == 0:
                self.session.headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.8',
                    'upgrade-insecure-requests': '1',
                    'user-agent': np.random.choice(self.userAgents)
                }
            if _type == "sell":
                resp = self.session.get(self.getForSellQuery(i))
            elif _type == "rent":
                resp = self.session.get(self.getForRentQuery(i))

            data = resp.json()
            self.id += 1
            df = pd.DataFrame(columns=self.hashColumns)
            tempDF = pd.json_normalize(data["cat1"]["searchResults"]["listResults"], sep="_")
            for colName in tempDF:
                df[colName] = tempDF[colName]
            df["hash_index"] = [hashlib.md5(str(tuple(i)).encode()).hexdigest() for i in df[self.hashColumns].itertuples(index=False)]
            df["LoadDate"] = datetime.datetime.now()
            df.set_index("hash_index", inplace=True)
            df.loc[set(df.index) - self.oldHash, self.fullColumns].to_csv(self.fileName, mode="a", header=False, index_label="hash_index")
            print(i)
            time.sleep(1 + 2 * np.random.uniform())


    def process(self, _type="sell"):
        self.sendInitRequest(_type)
        self.sendRequest(_type)