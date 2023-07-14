## Case 3

**Return the stolen cars!**

<img src="https://detective.kusto.io/_next/image?url=https://kda-webassets.azureedge.net/images/s2_case_003_48c588e3.png&w=750&q=75" width=35% height=35%>

> Hey there Detective,

> We've got an urgent case that needs your expertise! There has been a sudden increase in unsolved cases of stolen cars all across our city, and the police need our help again to crack the case.

> We've been given access to a massive dataset of car traffic for over a week, as well as a set of cars that have been stolen. It's possible that the car's identification plates were replaced during the robbery, which makes this case even more challenging.

> We need you to put on your detective hat and analyze the data to find any patterns or clues that could lead us to the location of these stolen cars. It is very likely that all the stolen cars are being stored in the same location.

> Time is of the essence, and we need to find these cars before they are sold or taken out of the city. The police are counting on us to solve this case, and we can't let them down!

> Are you up for the challenge, detective? We know you are! Let's get to work and crack this case wide open!

> Best regards,
> Captain Samuel Impson.

()[/img/KDA/Road.png]

Let's jump into the data and find the cars.

Copy and insert the below code to ingest the data into your cluster. 

```kusto
.execute database script <|
// It takes about 1min to script to complete loading the data
.set-or-replace StolenCars <|
datatable(VIN:string)
   ['LG232761G','SA732295L','MW406687Q','PR843817F','EL438126P',
    'GA871473A','IR177866Y','LP489241B','AS483204L','DO255727O',
    'BV850698T','YZ347238C','NJ586451R','VB724416I','SI241398E',
    'IN149152E','PV340883B','CK552050Z','ZJ786806D','KU388194T']
.create-merge table CarsTraffic(Timestamp:datetime, VIN:string, Ave:int, Street:int)
//clear any previously ingested data if such exists
.clear table CarsTraffic data
.ingest async into table CarsTraffic(@'https://kustodetectiveagency.blob.core.windows.net/kda2c3cartraffic/log_00000.csv.gz')
.ingest async into table CarsTraffic(@'https://kustodetectiveagency.blob.core.windows.net/kda2c3cartraffic/log_00001.csv.gz')
.ingest into table CarsTraffic(@'https://kustodetectiveagency.blob.core.windows.net/kda2c3cartraffic/log_00002.csv.gz')
```

We can directly see that we have a table with all of the stolen cars VIN number - StolenCars. 

```kusto
StolenCars
```

The other table CarsTraffic contains Timestamp, VIN, Ave, and Street. Do you remember the bank robbery case from season 1? 

```kusto
CarsTraffic
| take 10
```

So we have some stolen cars, their VIN plate are replaced and we do hope that the cars are gathered in the same location. 

I think we could try out the function arg_max to find some more clues. 

```kusto
CarsTraffic
| where VIN in (StolenCars)
| summarize arg_max(Timestamp,*) by VIN
```

Okey, so we have two different Ave and Street where our stolen cars are last seen. 

* Ave: 223, Street: 86
* Ave: 122, Street: 251

```kusto
CarsTraffic
| summarize arg_max(Timestamp,Ave,Street) by VIN
| summarize count() by Ave,Street
| sort by count_
```

The same Ave and Street appears in our result, more than 4900 counts. 

Let's try some join between our StolenCars and CarsTraffic to see if we can find something useful. 

```kusto
StolenCars
| join kind=inner CarsTraffic on VIN
| summarize arg_max(Timestamp, Ave, Street) by VIN
| summarize count() by Ave, Street
| sort by count_
```

Ah, we have the same Ave and Street with a total of 20 cars. I think we can be sure that we have the location of the swapping of plates. 

I have no clue how many minutes it take to switch the plates but around somewhere between 5-10 minutes sounds to be right for me. 

```kusto
let StolenVINs=(   
    StolenCars
    | join kind=inner CarsTraffic on VIN
    | summarize arg_max(Timestamp, Ave, Street) by VIN
    | project-rename StolenVINPlate=VIN
);
CarsTraffic
| summarize StartTime=arg_min(Timestamp, Ave,Street), EndTime=arg_max(Timestamp, Ave, Street) by VIN
| project-rename StartAve=Ave, StartStreet=Street,EndAve=Ave1, EndStreet=Street1
| join kind=inner StolenVINs on $left.StartAve == $right.Ave and $left.StartStreet == $right.Street 
| where StartTime between (Timestamp .. 8m)
| summarize Count=count() by EndAve, EndStreet
```
