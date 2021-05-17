PySpark script to filter out customers in specified country/countries.

Usage: python filter.py path/to/presonal_data.csv path/to/financial_data.csv country_name_1 country_name_2 .. country_name_N

Expected input tables format:
### personal data .csv format
```
id,first_name,last_name,email,country
1,Feliza,Eusden,feusden0@ameblo.jp,France
2,Priscilla,Le Pine,plepine1@biglobe.ne.jp,France
3,Jaimie,Sandes,jsandes2@reuters.com,France
4,Nari,Dolphin,ndolphin3@cbslocal.com,France
5,Garik,Farre,gfarre4@economist.com,France
```

### financial data .csv format
```
id,btc_a,cc_t,cc_n
1,1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2,visa-electron,4175006996999270
2,1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T,jcb,3587679584356527
3,1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd,diners-club-enroute,201876885481838
4,1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY,switch,564182038040530730
5,1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg,jcb,3555559025151828
```

## Results are written into .csv file in the following format:
```
client_identifier,email,bitcoin_address,credit_card_type
1,feusden0@ameblo.jp,1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2,visa-electron
2,plepine1@biglobe.ne.jp,1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T,jcb
3,jsandes2@reuters.com,1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd,diners-club-enroute
4,ndolphin3@cbslocal.com,1GNvinVKGzPBVNZScNA2jKnDSBs4R7Y3rY,switch
5,gfarre4@economist.com,1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg,jcb
```
