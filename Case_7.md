## Case 7

**Mission Connect**

<img src="https://detective.kusto.io/_next/image?url=https%3A%2F%2Fkda-webassets.azureedge.net%2Fimages%2Fs2_case_007_7e8af286.png&w=384&q=75" width=35% height=35%>

> Hi Detective,

> It's been awesome witnessing your progress. Seriously, you've climbed to new heights in uncovering the misdeeds of sly cyber-criminal, Krypto. We, the National Security Office (NSO), had our eyes on him for ages, and thanks to your information, we finally managed to track him down. I'll spare you the thrilling details, but guess what? Turns out our guy held a high-ranking position as City Manager in the Mayor's office, and he was tight with Ms. Gaia Budskott, the Mayor of Digitown. And yes, he's also the mastermind behind the infamous KUsto ANti-Detective Agency (Kuanda.org) that you brilliantly exposed. However, here's the unfortunate part: he slipped through the fingers of Digitown's law enforcement. Given the new international nature of the case, we (the NSO) are taking over.

> So, let's cut to the chase. Time is of the essence, and we need your expertise and experience to help us find the final destination of Krypto.
While we have gathered significant information about him, it is not enough to capture him. Our sources indicate that he was spotted at the Doha airport on August 11, 2023, between 03:30 AM and 05:30 AM (UTC). However, by the time our agents arrived, he had already made his escape, presumably utilizing a private jet. We have deployed dozens of officers to all potential landing destinations, but he has evaded us so far. We have a single lead that suggests Krypto may have attempted a plane-to-plane jump, given his skills as a wingsuit expert. Here is where we got stuck.

> Fortunately, we have you (and full access to the public and private jet plane schedules on this day). Your mission, should you choose to accept it, is to determine the destination to which Krypto has fled.

> Hoping to hear back from you soon,
> NSO Agent Stas Fistuko

Man... I really don't like GeoLocation cases, but I guess I have to do it. Let's start by ingesting the data:

```kusto
.execute database script <|
.create-merge table Flights(Timestamp:datetime, callsign:string, lat:real, lon:real, velocity:real, heading:real, vertrate:real, onground:bool, geoaltitude:real) 
.create-merge table Airports (Id:string, Ident:string, Type:string, Name:string, lat:real, lon:real, elevation_ft:long, iso_country:string, iso_region:string, municipality:string, gps_code:string, local_code:string)  
.ingest into table Airports (@'https://kustodetectiveagency.blob.core.windows.net/kda2c7flights/airports.csv.gz')
.ingest async into table Flights (@'https://kustodetectiveagency.blob.core.windows.net/kda2c7flights/flights_1.csv.gz')
.ingest async into table Flights (@'https://kustodetectiveagency.blob.core.windows.net/kda2c7flights/flights_2.csv.gz')
.ingest into table Flights (@'https://kustodetectiveagency.blob.core.windows.net/kda2c7flights/flights_3.csv.gz')
```

So we need to:

* Find Doha airport
* Find the flight that was in Doha airport between 03:30 AM and 05:30 AM (UTC)
* Assume that Krypto jumped from that flight to another flight
* Find the destination of the second flight

![](/img/AI_KDAS2E7_Airplane.png)

Okey, we now have two tables: `Flights` and `Airports`. 

```kusto
Flights
| take 10
```

```kusto
Airports
| take 10
```

Think you know the drill by now. Always good to look into the tables to see what we're dealing with.

Let's start by finding the Doha airport, I have no clue if the name "Doha" is the name of the airport or the city, so let's search for it:

```kusto
Airports
| search "Doha"
```

![](/img/Case7/Doha_Airport.png)

Gotya! It was the municipality. Now let's find the flight that was in Hamad International Airport between 03:30 AM and 05:30 AM (UTC):

We will use the Timestamp column and the between() function to find the flights that were in the airport between the times we want.

```kusto
Flights
| where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30))
```

We could also specify direct times into the between() function, such as 2h instead of writing the datetime() function again, but I prefer to be more specific.

```kusto
Flights
| where Timestamp between (datetime(2023-08-11 03:30) .. 2h)
```

Same, same.

Because we are looking between tables we must use the join operator. Now we will introduce the [geo_point_to_s2cell() function](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/geo-point-to-s2cell-function?WT.mc_id=AZ-MVP-5004683). I know, geo stuff, I cannot even find in my own city without an GPS telling me where to go. But this function is pretty cool, it takes a longitude and latitude and converts it to a S2 cell. S2 cells are a way to divide the world into smaller pieces.

In the join operator we want to have a key that we can join on, so we will use the geo_point_to_s2cell() function to create a key for each row in the table. We will use the default level of 11, but you can specify a level if you want to. We also want to add that the planes are onground, because we don't want to find the planes that are in the air.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
```

Almost 700 records. Let's add distinct callsign to the query to only see the unique airplanes.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign
```

19 airplanes. Getting closer Krypto!

Let's add the result to its own variable so we can use it later.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
```

Now we need to find the flight that Krypto jumped to. We will use the same method as before, but we will use the SuspiciousFlights variable in a where clause together with flights that was not on the ground.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
Flights
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| where callsign in (SuspiciousFlights) and onground == false and Timestamp > datetime(2023-08-11 05:30)
```

Too many results... 8415 records.

We need to join once more with the Flights table and with a where clause that have NotEquals <> to the callsigns. Let's rename some columns to make it easier to read.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
Flights
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| where callsign in (SuspiciousFlights) and onground == false and Timestamp > datetime(2023-08-11 03:30)
| join kind=inner (
    Flights
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
        | extend allFlights = callsign
        ) on key, Timestamp
| where callsign <> allFlights
```

1076 records. I do hope we can narrow it down a bit more.

Let's start assuming some things. This is where I was stuck for some time.

* The two flights are going the **same** direction (Heading column, took me a while to figure that out)
* The two flights have the same speed? (Velocity column, English is not my first language)
* The two flights have some difference in altitude

I was trying to look at the difference at both longitude and latitude but when I did understand the heading column it was much easier.

Kusto will be our friend in math here.  

```kusto
| extend HeadingDiff = heading - heading1
| extend VelocityDiff = velocity - velocity1
| extend AltitudeDiff = geoaltitude - geoaltitude1
```

Looking at the three new columns we have a mix of result. Should we try to round things up? I don't know, let's try it.

```kusto
| extend HeadingDiff = round(heading - heading1, 0)
| extend VelocityDiff = round(velocity - velocity1, 0)
| extend AltitudeDiff = round(geoaltitude - geoaltitude1, 0)
```

I think it's better output now.

To answer the first assumption, we need to find the flights that have a heading difference of 0. We will use the where clause to filter out the result.

```kusto
| where HeadingDiff == 0
```

67 records. Watch out Krypto, we're coming for you!

To answer the second assumption, we need to find the flights that have a very close velocity difference, maybe a value of (less than) 10? We will use the where clause to filter out the result.

```kusto
| where HeadingDiff == 0 and VelocityDiff < 10
```

50 records. Getting closer. Krypto you can run, but you can't hide!

To answer the third assumption, we need to find the flights that have a very close altitude difference, maybe a value of (less than) 2000 but more than 100? We will use the where clause to filter out the result.

```kusto
| where HeadingDiff == 0 and VelocityDiff < 10 and AltitudeDiff < 1000
```

The entire query right now:

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
Flights
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| where callsign in (SuspiciousFlights) and onground == false and Timestamp > datetime(2023-08-11 03:30)
| join kind=inner (
    Flights
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
        | extend allFlights = callsign
        ) on key, Timestamp
| where callsign <> allFlights
| extend HeadingDiff = round(heading - heading1, 0)
| extend VelocityDiff = round(velocity - velocity1, 0)
| extend AltitudeDiff = round(geoaltitude - geoaltitude1, 0)
| where HeadingDiff == 0 and VelocityDiff < 10 and AltitudeDiff between (100 .. 2000)
```

7 records. I think we got it! Now we need to find out where this flight is going. We will make a new join with the Flights table to get the last record of that flight with the geo data then use the lookup operator to find the airport.

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
Flights
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| where callsign in (SuspiciousFlights) and onground == false and Timestamp > datetime(2023-08-11 03:30)
| join kind=inner (
    Flights
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
        | extend allFlights = callsign
        ) on key, Timestamp
| where callsign <> allFlights
| extend HeadingDiff = round(heading - heading1, 0)
| extend VelocityDiff = round(velocity - velocity1, 0)
| extend AltitudeDiff = round(geoaltitude - geoaltitude1, 0)
| where HeadingDiff == 0 and VelocityDiff < 10 and AltitudeDiff between (100 .. 2000)
| project callsign = callsign1
| join kind=inner (
    Flights
    ) on callsign
| top 1 by Timestamp desc
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| lookup (
            Airports 
            | extend key=geo_point_to_s2cell(lon, lat, s2_level)
        ) on key
```

Hey NSO, send all units to the *Josep Tarradellas Barcelona-El Prat Airport* in Barcelona, Spain. We got him!

Now I'm super curious about the jump, where it took place, when and how.

```kusto
Flights
| where callsign in ("OJIT393", "HFID97")
| sort by callsign, Timestamp asc
| extend Map=geo_point_to_s2cell(lon, lat, 14)
| project geo_s2cell_to_central_point(Map), callsign
| render scatterchart with (kind=map)
```

![](/img/Case7/Krypto_jump.png)

![](/img/Case7/Krypto_jump_zoom.png)

And the time?

```kusto
let s2_level = 11; // 11 is also the default value if you don't specify it
let SuspiciousFlights =
Airports
| where municipality has "Doha"
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| join kind=inner (
    Flights
        | where Timestamp between (datetime(2023-08-11 03:30) .. datetime(2023-08-11 05:30)) and onground
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
    ) on key
| distinct callsign;
Flights
| extend key=geo_point_to_s2cell(lon, lat, s2_level)
| where callsign in (SuspiciousFlights) and onground == false and Timestamp > datetime(2023-08-11 03:30)
| join kind=inner (
    Flights
        | extend key=geo_point_to_s2cell(lon, lat, s2_level)
        | extend allFlights = callsign
        ) on key, Timestamp
| where callsign <> allFlights
| extend HeadingDiff = round(heading - heading1, 0)
| extend VelocityDiff = round(velocity - velocity1, 0)
| extend AltitudeDiff = round(geoaltitude - geoaltitude1, 0)
| where HeadingDiff == 0 and VelocityDiff < 10 and AltitudeDiff between (100 .. 2000)
```

Between 11:44:00 - 11:47:30. As you can see on the map, the airplan took a very hard turn to the left after the jump... 

And the how?

I have no clue... Crazy man.

![](/img/Case7/Krypto_jumping.png)