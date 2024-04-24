#!/usr/bin/env python
# coding: utf-8

# In[54]:


import pandas as pd
import numpy as np


#Population Data Set 

indiaPopulation = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Population/population.csv")
indiaPopulation = indiaPopulation.drop(["1951", "1961", "1971", "1981", "1991", "2021"], axis = 1)
indiaPopulation = indiaPopulation.rename(columns = {"state": "State"})
indiaPopulation = indiaPopulation.loc[indiaPopulation["State"].isin(states)]
years =[str(x) for x in range(2002,2011)]
indiaPopulation[years] = indiaPopulation["2012"]= float('nan')
popped = indiaPopulation.pop("2011")
poppeddata = indiaPopulation.pop("data")
indiaPopulation.insert(loc = 11, column = "2011", value = popped)
indiaPopulation.insert(loc = 13, column = "data", value = poppeddata)

#Fill blanks with an consistent increment
for index, row in indiaPopulation.iterrows():
    initial = row["2001"]
    final = row["2011"]
    diff = final - initial
    interval = diff / 10
    for year in range(2002, 2013):
        if year != "2011":
            indiaPopulation.at[index, str(year)] = indiaPopulation.at[index, str(year - 1)] + interval
            
#Create microsets of data
totalPopulation = indiaPopulation.loc[indiaPopulation["data"] == "Total Population"] 
totalPopulation = totalPopulation.melt(id_vars=["State"], var_name="Year", value_name="Total Population")
totalPopulation = totalPopulation.loc[totalPopulation["Year"] != "data"]
totalPopulation['Year'] = totalPopulation['Year'].astype('int64')

ruralPopulation = indiaPopulation.loc[indiaPopulation["data"] == "Population in Rural Area"] 
ruralPopulation = ruralPopulation.melt(id_vars=["State"], var_name="Year", value_name="Rural Population ")
ruralPopulation = ruralPopulation.loc[ruralPopulation["Year"] != "data"]
ruralPopulation.to_csv("ruralPopulation.csv", index = False)

decGrowth = indiaPopulation.loc[indiaPopulation["data"] == "Decadal Growth"] 
decGrowth = decGrowth.melt(id_vars=["State"], var_name="Year", value_name="Decadal Growth ")
decGrowth = decGrowth.loc[decGrowth["Year"] != "data"]
decGrowth.to_csv("decadalGrowth.csv", index = False)



df = pd.read_csv("Suicides in India 2001-2012.csv")


#Suicide Data

states = ["Maharashtra", "Andhra Pradesh", "Karnataka","Madhya Pradesh", "Kerala", "Tamil Nadu", 
          "West Bengal", "Uttar Pradesh", "Gujarat", "Assam", "Haryana", "Rajasthan"]

allSuicides = df[(df["Total"] != 0) & (df["Type_code"]=="Means_adopted")
   & (df["State"].isin(states))]
allSuicides = allSuicides.drop(["Type_code"], axis = 1)
allSuicides = pd.merge(allSuicides, totalPopulation, on = ["State", "Year"], how = "left")
allSuicides["totalPerCapita"]  = allSuicides["Total"] / allSuicides["Total Population"]
allSuicides.to_csv("allSuicides.csv", index = False)

allMaleSuicides = df[(df["Total"] != 0) & (df["Type_code"]=="Means_adopted") 
   & (df["Gender"] == "Male")
   & (df["State"].isin(states))]
allMaleSuicides = allMaleSuicides.drop(["Type_code"], axis = 1)
allMaleSuicides = pd.merge(allMaleSuicides, totalPopulation, on = ["State", "Year"], how = "left")
allMaleSuicides["totalPerCapita"]  = allMaleSuicides["Total"] / allMaleSuicides["Total Population"]
allMaleSuicides.to_csv("allMaleSuicides.csv", index = False)

maleFarmersOnly = df[(df["Total"] != 0) & (df["Type_code"]=="Means_adopted")
   & (df["Gender"]=="Male") & (df["Type"] == "By Consuming Insecticides")
   & (df["State"].isin(states)) ]
maleFarmersOnly = maleFarmersOnly.drop(["Type_code"], axis = 1)
maleFarmersOnly = pd.merge(maleFarmersOnly, totalPopulation, on = ["State", "Year"], how = "left")
maleFarmersOnly["totalPerCapita"]  = maleFarmersOnly["Total"] / maleFarmersOnly["Total Population"]
maleFarmersOnly.to_csv("maleFarmersOnly.csv", index = False)

allFarmers = df[(df["Total"] != 0) & (df["Type_code"]=="Means_adopted") & (df["Type"] == "By Consuming Insecticides")
   & (df["State"].isin(states)) ]
allFarmers = allFarmers.drop(["Type_code"], axis = 1)
allFarmers = pd.merge(allFarmers, totalPopulation, on = ["State", "Year"], how = "left")
allFarmers["totalPerCapita"]  = allFarmers["Total"] / allFarmers["Total Population"]
allFarmers.to_csv("allFarmers.csv", index = False)


#RainfallData

rainfalldf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Rainfall/Rainfall.csv')

years = range(2001,2013)
rainfall1 = rainfalldf[["YEAR", "States/UTs", "ANNUAL"]]
newrainfall = rainfall1[rainfall1["YEAR"].isin(years)]
newrainfall = newrainfall.rename(columns={"States/UTs": "State", "ANNUAL": "Annual Rainfall", "YEAR": "Year"})  # Reassign the modified DataFrame
newrainfall

#Andhra Pradesh merging
andhra_pradesh_rainfall = newrainfall[newrainfall["State"].isin(["Telangana", "Coastal Andhra Pradesh", "Rayalseema"])].groupby("Year").sum()
andhra_pradesh_rainfall["State"] = "Andhra Pradesh"
newrainfall = pd.concat([newrainfall, andhra_pradesh_rainfall.reset_index()], ignore_index=True)
newrainfall = newrainfall[~newrainfall["State"].isin(["Telangana", "Coastal Andhra Pradesh", "Rayalseema"])]

#West Bengal Merging
westBengalrainfall = newrainfall[newrainfall["State"].isin(["Gangetic West Bengal", "Sub Himalayan West Bengal & Sikkim"])].groupby("Year").sum()
westBengalrainfall["State"] = "West Bengal"
newrainfall = pd.concat([newrainfall, westBengalrainfall.reset_index()], ignore_index=True)
newrainfall = newrainfall[~newrainfall["State"].isin(["Gangetic West Bengal", "Sub Himalayan West Bengal & Sikkim"])]

#Uttar Pradesh Merging
UPrainfall = newrainfall[newrainfall["State"].isin(["East Uttar Pradesh", "West Uttar Pradesh"])].groupby("Year").sum()
UPrainfall["State"] = "Uttar Pradesh"
newrainfall = pd.concat([newrainfall, UPrainfall.reset_index()], ignore_index=True)
newrainfall = newrainfall[~newrainfall["State"].isin(["East Uttar Pradesh", "West Uttar Pradesh"])]

#Rajasthan Merging
rajRainfall = newrainfall[newrainfall["State"].isin(["West Rajasthan", "East Rajasthan"])].groupby("Year").sum()
rajRainfall["State"] = "Rajasthan"
newrainfall = pd.concat([newrainfall, rajRainfall.reset_index()], ignore_index = True)
newrainfall = newrainfall.loc[~newrainfall["State"].isin(["West Rajasthan", "East Rajasthan"])]

#Madhya Pradesh Merging
MPRainfall = newrainfall[newrainfall["State"].isin(["West Madhya Pradesh", "East Madhya Pradesh"])].groupby("Year").sum()
MPRainfall["State"] = "Madhya Pradesh"
newrainfall = pd.concat([newrainfall, MPRainfall.reset_index()], ignore_index = True)
newrainfall = newrainfall.loc[~newrainfall["State"].isin(["West Madhya Pradesh", "East Madhya Pradesh"])]

#Karnataka merging
Karnataka_rainfall = newrainfall[newrainfall["State"].isin(["Coastal Karnataka", "South Interior Karnataka", "North Interior Karnataka"])].groupby("Year").sum()
Karnataka_rainfall["State"] = "Karnataka"
newrainfall = pd.concat([newrainfall, Karnataka_rainfall.reset_index()], ignore_index=True)
newrainfall = newrainfall[~newrainfall["State"].isin(["Coastal Karnataka", "South Interior Karnataka", "North Interior Karnataka"])]

#Maharashtra rename
newrainfall.loc[newrainfall["State"]== "Madhya Maharashtra", "State"] = "Maharashtra"

#Gujarat rename
newrainfall.loc[newrainfall["State"]=="Gujarat Region", "State"] = "Gujarat"

#Haryana rename
newrainfall.loc[newrainfall["State"]=="Haryana Delhi & Chandigarh", "State"] = "Haryana"

#Assam rename
newrainfall.loc[newrainfall["State"] == "Assam & Meghalaya", "State"] = "Assam"

newrainfall

#Cross checking to make sure no messed up data

East = np.array([551.2, 285.3, 626.6, 606.2, 567.9, 748.4, 575.7, 644.6, 414.1, 689.4, 802.1, 693.6])

West = np.array([285.3, 92.4, 368.1, 196.7, 260.8, 361.3, 310.8, 324.1, 160.4, 473.2, 418.7, 327.3])

Rajasthan = East + West

Rajasthan

#To CSV
newrainfall.to_csv("NewRainfallData.csv", index=False)



#Temperature Anomaly Data

#Assam Weather
assamWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/AssamWeatherData.csv')
assamWeatherdf = assamWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
AssamWeather = assamWeatherdf.groupby(["Year", "State"]).mean().reset_index()
AssamWeather = AssamWeather.drop(["Month"], axis = 1)
AssamWeather.to_csv("AssamTemp.csv", index=False) 

#Andhra Weather
APWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/AndhraPradeshWeather.csv')
APWeatherdf = APWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
APWeather = APWeatherdf.groupby(["Year", "State"]).mean().reset_index()
APWeather = APWeather.drop(["Month"], axis = 1)
APWeather.to_csv("APTemp.csv", index=False) 

#Gujarat Weather
gujaratWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/GujaratWeatherData.csv')
gujaratWeatherdf  = gujaratWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
GujaratWeather = gujaratWeatherdf.groupby(["Year", "State"]).mean().reset_index()
GujaratWeather = GujaratWeather.drop(["Month"], axis = 1)
GujaratWeather.to_csv("GujaratTemp.csv", index=False)

#Haryana Weather
HaryanaWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/HaryanaWeatherData.csv')
HaryanaWeatherdf  = HaryanaWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
HaryanaWeather = HaryanaWeatherdf.groupby(["Year", "State"]).mean().reset_index()
HaryanaWeather = HaryanaWeather.drop(["Month"], axis = 1)
HaryanaWeather.to_csv("HaryanaTemp.csv", index=False)

#Karnataka Weather
KarnatakaWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/KarnatakaWeatherData.csv')                             
KarnatakaWeatherdf  = KarnatakaWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
KarnatakaWeather = KarnatakaWeatherdf.groupby(["Year", "State"]).mean().reset_index()
KarnatakaWeather = KarnatakaWeather.drop(["Month"], axis = 1)
KarnatakaWeather.to_csv("KarnatakaTemp.csv", index=False)

#Kerala Weather
KeralaWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/KeralaWeatherData.csv')                             
KeralaWeatherdf  = KeralaWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
KeralaWeather = KeralaWeatherdf.groupby(["Year", "State"]).mean().reset_index()
KeralaWeather = KeralaWeather.drop(["Month"], axis = 1)
KeralaWeather.to_csv("KeralaTemp.csv", index=False)

#Madhya Weather
MPWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/MadhyaPradeshWeather.csv')
MPWeatherdf = MPWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
MPWeather = MPWeatherdf.groupby(["Year", "State"]).mean().reset_index()
MPWeather = MPWeather.drop(["Month"], axis = 1)
MPWeather.to_csv("MPTemp.csv", index=False) 

#Maharashtra Weather
MaharashtraWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/MaharashtraWeatherData.csv')
MaharashtraWeatherdf = MaharashtraWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
MaharashtraWeather = MaharashtraWeatherdf.groupby(["Year", "State"]).mean().reset_index()
MaharashtraWeather = MaharashtraWeather.drop(["Month"], axis = 1)
MaharashtraWeather.to_csv("MaharashtraTemp.csv", index=False) 

#Rajasthan Weather
RajasthanWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/RajasthanWeatherData.csv')
RajasthanWeatherdf = RajasthanWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
RajasthanWeather = RajasthanWeatherdf.groupby(["Year", "State"]).mean().reset_index()
RajasthanWeather = RajasthanWeather.drop(["Month"], axis = 1)
RajasthanWeather.to_csv("RajasthanTemp.csv", index=False) 

#Tamil Nadu Weather
TamilNaduWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/TamilNaduWeatherData.csv')
TamilNaduWeatherdf = TamilNaduWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
TamilNaduWeather = TamilNaduWeatherdf.groupby(["Year", "State"]).mean().reset_index()
TamilNaduWeather = TamilNaduWeather.drop(["Month"], axis = 1)
TamilNaduWeather.to_csv("TamilNaduTemp.csv", index=False)

#Uttar Weather
UPWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/UttarPradeshWeather.csv')
UPWeatherdf = UPWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
UPWeather = UPWeatherdf.groupby(["Year", "State"]).mean().reset_index()
UPWeather = UPWeather.drop(["Month"], axis = 1)
UPWeather.to_csv("UPTemp.csv", index=False) 

#West Bengal Weather
WestBengalWeatherdf = pd.read_csv('/Users/abhinamala/Downloads/ECONOMETRICS DATA/Weather CSVs/Old-/WestBengalWeather.csv')
WestBengalWeatherdf = WestBengalWeatherdf.drop(["v6", "v7", "v8", "v9", "v10", "v11"], axis=1)
WestBengalWeather = WestBengalWeatherdf.groupby(["Year", "State"]).mean().reset_index()
WestBengalWeather = WestBengalWeather.drop(["Month"], axis = 1)
WestBengalWeather.to_csv("WestBengalTemp.csv", index=False)

AllStatesTemperatureAnomalies = pd.concat([AssamWeather,APWeather, GujaratWeather, HaryanaWeather, KarnatakaWeather,
                                          KeralaWeather, MPWeather, MaharashtraWeather, RajasthanWeather,
                                          TamilNaduWeather, UPWeather, WestBengalWeather], ignore_index=True)
AllStatesTemperatureAnomalies.to_csv("StateTemperatureAnomalies.csv", index = False)



#State-wise all crop data
cropDf = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Production/APY.csv")

states = ["Maharashtra", "Andhra Pradesh", "Karnataka","Madhya Pradesh", "Kerala", "Tamil Nadu", 
          "West Bengal", "Uttar Pradesh", "Gujarat", "Assam", "Haryana", "Rajasthan"]
years = range(2001,2013)

cropDf = cropDf.loc[cropDf["State"].isin(states) & cropDf["Crop_Year"].isin(years)]
cropDf = cropDf.drop(["Crop", "Season", "District "], axis = 1)
cropDf = cropDf.groupby(["State", "Crop_Year"]).sum().reset_index()
cropDf = cropDf.rename(columns={'Crop_Year': 'Year', 'Yield': 'Total Yield', 'Production': 'Total Production', 'Area ' : 'Total Area'}, inplace=False)
cropDf.to_csv("CropYieldTotals_Statewise.csv", index = False)
cropDf


#State-wise most popular crop data
cropdf = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Production/APY.csv")
states = ["Maharashtra", "Andhra Pradesh", "Karnataka","Madhya Pradesh", "Kerala", "Tamil Nadu", 
          "West Bengal", "Uttar Pradesh", "Gujarat", "Assam", "Haryana", "Rajasthan"]
years = range(2001,2013)
statesDict = {
    "Haryana": "Wheat",
    "Uttar Pradesh": "Sugarcane",
    "Maharashtra": "Sugarcane",
    "Madhya Pradesh": "Wheat",
    "Rajasthan": "Wheat",
    "West Bengal": "Rice",
    "Andhra Pradesh": "Rice",
    "Tamil Nadu": "Rice",
    "Karnataka": "Rice",
    "Gujarat": "Cotton(lint)",
    "Kerala": "Rice",
    "Assam": "Rice"
}

listKeys = list(statesDict.keys())
listValues = list(statesDict.values())

#Filtering only the most popular crops
cropdf = cropdf.loc[cropdf["State"].isin(states) & cropdf["Crop_Year"].isin(years)]
cropdf = cropdf[cropdf.apply(lambda x: x["Crop"] == statesDict.get(x["State"]), axis=1)]
cropdf = cropdf.groupby(["State", "Crop_Year"]).sum().reset_index()
cropdf = cropdf.rename(columns={'Crop_Year': 'Year', 'Yield': 'MC Yield', 'Production': 'MC Production', 'Area ' : 'MC Area', 'Crop' : 'Most Cultivated Crop'})
cropdf["Crop"] = cropdf.apply(lambda x: statesDict[x["State"]], axis=1)
cropdf = cropdf.drop(["Season", "District ", "Crop"], axis = 1)
cropdf["Most Cultivated Crop"] = cropdf["State"].map(statesDict)
cropdf.to_csv("MostCultivatedCropByState.csv", index = False)


#Literacy Rates

litRates = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Literacy Rates/literacy_rate.csv")
litRates = litRates.rename(columns={"States/Union_Territorries": "State"})
litRates = litRates.loc[litRates["State"].isin(states)].reset_index()
litRates = litRates.drop(["No.","1951", "1961", "1971", "1981", "1991", "index"], axis = 1)
years =[str(x) for x in range(2002,2011)]
litRates[years] = litRates["2012"]= float('nan')
popped = litRates.pop("2011")
litRates.insert(loc = 11, column = "2011", value = popped)
new = litRates["2011"] - litRates["2001"]

#Fill blanks with an consistent increment
for index, row in litRates.iterrows():
    initial = row["2001"]
    final = row["2011"]
    diff = final - initial
    interval = diff / 10
    for year in range(2002, 2013):
        if year != "2011":
            litRates.at[index, str(year)] = litRates.at[index, str(year - 1)] + interval
            
#Reshape
litRates = litRates.melt(id_vars=["State"], var_name="Year", value_name="LitRate")
litRates = litRates.sort_values(by=["State", "Year"])
litRates = litRates.reset_index(drop=True)

litRates.to_csv("StateLiteracyRates2001-2012.csv", index = False)


#State-wise Economic Data
indiaEconomicData = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Suicide Data/india.csv")
indiaEconomicData = indiaEconomicData.rename(columns = {"state":"State", "CATEGORY":"Category"})
states = ["Maharashtra", "Andhra Pradesh", "Karnataka","Madhya Pradesh", "Kerala", "Tamil Nadu", 
          "West Bengal", "Uttar Pradesh", "Gujarat", "Assam", "Haryana", "Rajasthan"]
years = range(2001,2013)
stryears = [str(x) for x in years]
stryears.extend(["State", "Category"])
filtered_columns = [col for col in indiaEconomicData.columns if any(year in col for year in stryears)]
indiaEconomicData = indiaEconomicData[filtered_columns]
indiaEconomicData = indiaEconomicData.loc[indiaEconomicData["State"].isin(states)]


#Specific Metrics

perCapitaIncome = indiaEconomicData.loc[indiaEconomicData["Category"] == "Per Capita Income"]
perCapitaIncome = perCapitaIncome.melt(id_vars=["State"], var_name="Year", value_name="Per Capita Income")
perCapitaIncome = perCapitaIncome.loc[perCapitaIncome["Year"]!= "Category"]

valueAddedAgriculture = indiaEconomicData.loc[indiaEconomicData["Category"] == "Value Added by Agriculture"]
valueAddedAgriculture = valueAddedAgriculture.melt(id_vars=["State"], var_name="Year", value_name="Value Added by Agriculture")
valueAddedAgriculture = valueAddedAgriculture.loc[valueAddedAgriculture["Year"]!= "Category"]

ndsp = indiaEconomicData.loc[indiaEconomicData["Category"] == "Net State Domestic Product"]
ndsp = ndsp.melt(id_vars=["State"], var_name="Year", value_name="Net State Domestic Product")
ndsp = ndsp.loc[ndsp["Year"]!= "Category"]

#To CSV
perCapitaIncome.to_csv("perCapitaIncome.csv", index = False)
valueAddedAgriculture.to_csv("valueAddedAgriculture.csv", index = False)
ndsp.to_csv("ndsp.csv", index = False)


#Megadata for all suicides

allSuicidesIndia = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/allSuicides.csv")
lit = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateLiteracyRates2001-2012.csv",encoding='latin1')
rain = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/NewRainfallData.csv",encoding='latin1')
temp = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateTemperatureAnomalies.csv",encoding='latin1')
ytotal = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/CropYieldTotals_Statewise.csv",encoding='latin1')
mostcult = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/MostCultivatedCropByState.csv",encoding='latin1')
ndsp1 = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ndsp.csv", encoding = "latin1")
perCapitaInc = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/perCapitaIncome.csv", encoding = "latin1")
decadalGrowth = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/decadalGrowth.csv", encoding = "latin1")
rural = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ruralPopulation.csv", encoding = "latin1")
valAdded =  pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/valueAddedAgriculture.csv", encoding = "latin1")

finalAllSuicides = pd.merge(allSuicidesIndia, lit, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, rain, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, temp, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, ytotal, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, mostcult, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, ndsp1, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, perCapitaInc, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, decadalGrowth, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, rural, on = ["State", "Year"], how = "left")
finalAllSuicides = pd.merge(finalAllSuicides, valAdded, on = ["State", "Year"], how = "left")

finalAllSuicides.to_csv("allSuicidesIndia.csv", index = False)


#Megadata for all male suicides

allMaleSuicidesIndia = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/allMaleSuicides.csv")
lit = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateLiteracyRates2001-2012.csv",encoding='latin1')
rain = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/NewRainfallData.csv",encoding='latin1')
temp = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateTemperatureAnomalies.csv",encoding='latin1')
ytotal = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/CropYieldTotals_Statewise.csv",encoding='latin1')
mostcult = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/MostCultivatedCropByState.csv",encoding='latin1')
ndsp1 = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ndsp.csv", encoding = "latin1")
perCapitaInc = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/perCapitaIncome.csv", encoding = "latin1")
decadalGrowth = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/decadalGrowth.csv", encoding = "latin1")
rural = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ruralPopulation.csv", encoding = "latin1")
valAdded =  pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/valueAddedAgriculture.csv", encoding = "latin1")

finalMaleSuicides = pd.merge(allMaleSuicidesIndia, lit, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, rain, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, temp, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, ytotal, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, mostcult, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, ndsp1, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, perCapitaInc, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, decadalGrowth, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, rural, on = ["State", "Year"], how = "left")
finalMaleSuicides = pd.merge(finalMaleSuicides, valAdded, on = ["State", "Year"], how = "left")

finalMaleSuicides.to_csv("maleSuicidesIndia.csv", index = False)


#Megadata for all farmer suicides

allFarmerSuicidesIndia = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/allFarmers.csv")
lit = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateLiteracyRates2001-2012.csv",encoding='latin1')
rain = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/NewRainfallData.csv",encoding='latin1')
temp = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateTemperatureAnomalies.csv",encoding='latin1')
ytotal = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/CropYieldTotals_Statewise.csv",encoding='latin1')
mostcult = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/MostCultivatedCropByState.csv",encoding='latin1')
ndsp1 = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ndsp.csv", encoding = "latin1")
perCapitaInc = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/perCapitaIncome.csv", encoding = "latin1")
decadalGrowth = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/decadalGrowth.csv", encoding = "latin1")
rural = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ruralPopulation.csv", encoding = "latin1")
valAdded =  pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/valueAddedAgriculture.csv", encoding = "latin1")

finalfarmerSuicides = pd.merge(allFarmerSuicidesIndia, lit, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, rain, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, temp, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, ytotal, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, mostcult, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, ndsp1, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, perCapitaInc, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, decadalGrowth, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, rural, on = ["State", "Year"], how = "left")
finalfarmerSuicides = pd.merge(finalfarmerSuicides, valAdded, on = ["State", "Year"], how = "left")

finalfarmerSuicides.to_csv("allFarmerSuicidesIndia.csv", index = False)


#Megadata for all male farmer suicides

allMaleFarmerSuicidesIndia = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/maleFarmersOnly.csv")
lit = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateLiteracyRates2001-2012.csv",encoding='latin1')
rain = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/NewRainfallData.csv",encoding='latin1')
temp = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/StateTemperatureAnomalies.csv",encoding='latin1')
ytotal = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/CropYieldTotals_Statewise.csv",encoding='latin1')
mostcult = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/MostCultivatedCropByState.csv",encoding='latin1')
ndsp1 = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ndsp.csv", encoding = "latin1")
perCapitaInc = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/perCapitaIncome.csv", encoding = "latin1")
decadalGrowth = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/decadalGrowth.csv", encoding = "latin1")
rural = pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/ruralPopulation.csv", encoding = "latin1")
valAdded =  pd.read_csv("/Users/abhinamala/Downloads/ECONOMETRICS DATA/Final Data/valueAddedAgriculture.csv", encoding = "latin1")

finalMalefarmerSuicides = pd.merge(allMaleFarmerSuicidesIndia, lit, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, rain, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, temp, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, ytotal, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, mostcult, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, ndsp1, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, perCapitaInc, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, decadalGrowth, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, rural, on = ["State", "Year"], how = "left")
finalMalefarmerSuicides = pd.merge(finalMalefarmerSuicides, valAdded, on = ["State", "Year"], how = "left")

finalMalefarmerSuicides.to_csv("maleFarmerSuicidesIndia.csv", index = False)





