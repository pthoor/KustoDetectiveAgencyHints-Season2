## Case 4

**Tripple trouble!**

<img src="https://detective.kusto.io/_next/image?url=https://kda-webassets.azureedge.net/images/s2_case_004_4d75ce23.png&w=750&q=75" width=35% height=35%>

> Dear Kusto Detective Agent,

> It's me, Gaia Budskott, the mayor of Digitown... I am writing to you because I am in desperate need of your help.

> Recently, I have been caught up in not one, not two, but three different police investigations. The police suspect that I am behind a series of crimes that I have nothing to do with. My personal electricity and billing account was apparently undercharged for the past few months, and phishing calls were made from numbers associated with my office. To top it off, secret documents that belong to me were found in a garage where stolen cars were placed.

> I am at my wit's end and I don't know who to turn to. I can't ask the police for help because they think I'm the one behind all of this. That's why I'm turning to you - perhaps you can help me figure out who is really behind all of this.

> I suspect that someone has hacked into the Digitown municipality system and stolen these documents. Our system is a known data hub and hosts various information about the town itself, real-time monitoring systems of the city, tax payments, etc. It serves as a real-time data provider to many organizations around the world, so it receives a lot of traffic. 

> Unfortunately, I don't have much data to give you. All I have is a 30-day traffic statistics report captured by the Digitown municipality system network routers. 

> I am hoping that your expertise and knowledge in big data analytics can help shed some light on who is behind these crimes and clear my name.

> Please, can you help me?

> Sincerely,
> Gaia

![](/img/KDA/Mayor_newspaper.png)

Now, insert the data. 

```kusto
.execute database script <|
.create-merge table IpInfo (IpCidr:string, Info:string)
//clear any previously ingested data if such exists
.clear table IpInfo data
.ingest into table IpInfo (@'https://kustodetectiveagency.blob.core.windows.net/kda2c4network/ip-lookup.csv.gz')
.create-merge table NetworkMetrics (Timestamp:datetime, ClientIP:string, TargetIP:string, BytesSent:long, BytesReceived:long, NewConnections:int)
.clear table NetworkMetrics data
.ingest async into table NetworkMetrics (@'https://kustodetectiveagency.blob.core.windows.net/kda2c4network/log_00000.csv.gz')
.ingest async into table NetworkMetrics (@'https://kustodetectiveagency.blob.core.windows.net/kda2c4network/log_00001.csv.gz')
// Last command is running sync, so when it finishes the data is already ingested.
// It can take about 1.5min to run.
.ingest into table NetworkMetrics (@'https://kustodetectiveagency.blob.core.windows.net/kda2c4network/log_00002.csv.gz')
```

Let's see what we have in the traffic logs that the mayor sent to us. We have the NetworkMetrics and IpInfo table. I do believe that we need to do some join operation between those two tables.

```kusto
IpInfo
| take 10
```

```kusto
NetworkMetrics
| take 10
```

