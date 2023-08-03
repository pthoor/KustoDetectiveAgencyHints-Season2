## Case 6

**Hack this rack!**

<img src="https://detective.kusto.io/_next/image?url=https://kda-webassets.azureedge.net/images/s2_case_006_e4e42ec5.png&w=750&q=75" width=35% height=35%>

> Hey there! I've got some juicy details for you regarding the elusive https://kuanda.org

> So, the bad news is that despite my best efforts, I still don't have a ton of info on these guys. But, the good news is that I did stumble upon a lead that might just crack this case wide open! You ready for this? Kuanda.org isn't just some run-of-the-mill fishing organization. Nope, my sources tell me it's a brand spanking new cyber organization that's all about digital data repositories. Talk about cutting-edge technology, am I right?

> But wait, it gets even better. They've been recruiting cyber-crime specialists like there's no tomorrow. Which means that this organization is serious about their work, and they have something big planned. And here's the kicker - every new member has to spend a week at the National Gallery of Art! Yeah, you heard that right. The same National Gallery of Art that houses all those fancy paintings and sculptures. What could they possibly be doing in there for a whole week? Studying Leonardo da Vinci's brushstrokes? I smell something fishy, and it's not just the art restoration chemicals.

> And to top it all off, my sources managed to snag some instructions for the new recruiters. If you can decode them, you might just have a shot at infiltrating their system and finding out what they're really up to. Who knows, you might even find the smoking gun that proves they're behind all those cyber-crimes. Good luck, detective - I sense you will need one!

> Cheers,
> El Puente

![](/img/KDA/Mayor_smile.png)

**Instructions for the new recruiters**

```text
12204/497 62295/24 50883/678 47108/107 193867/3,
45534/141 hidden 100922/183 143461/1 1181/505 46187/380.
41526/155 66447/199 30241/114, 33745/154 12145/387 46437/398 177191/131:
293/64 41629/1506 210038/432, 41612/803 216839/1.

404/258 rules 40/186 1472/222 122894/2 46081/105:
41594/650 32579/439 44625/141 184121/19 33254/348 357/273 32589/821,
46171/687 punctuations 62420/10 50509/48 1447/128,
176565/82'56721/591 561/225 insensitive, 30744/129 76197/32.

1319/42 41599/216 68/457 136016/146, 42420/126'46198/389 42429/158 40091/108 41667/252,
1515/555 177593/223 176924/73 45889/65 159836/96 35080/384 32578/199.
1607/167 124996/9 71/56, 1303/187 45640/1114 72328/247 75802/11,
1168/146 163380/12 57541/116 206122/738 365/267 46026/211 46127/19.

119295/425 45062/128 12198/133 163917/238 45092/8 54183/4 42453/82:
561/433 9/387 37004/287 1493/118 41676/38 163917/238 3159/118 63264/687
1/905 1493/109 43723/252, 136355/1 1159/134 40062/172 32588/604,
158574/1 45411/8 10/892 127587/175 - 633/9 72328/247 1514/615 42940/138.

164958/84 221014/479 151526/7 111124/138, 41668/206 34109/46 1514/555,
147789/2 3228/152 993/323 166477/167 178042/167, 50753/91'207786/8 12/372.
1108/158'42423/150 12/309 66154/9 213566/11 44981/158 1197/300
40184/149 92994/63-71071/179 75093/7 211718/18 74211/5 46144/399.
```

Import the data with:

```kusto
.execute database script <|
// The data was obtained from the repository of the National Gallery of Art:
// https://github.com/NationalGalleryOfArt/opendata
.create-merge table NationalGalleryArt (ObjectId:long, Title:string, BeginYear:long, EndYear:long, Medium:string, Inscription:string, Attribution:string, AssistiveText:string, ProvenanceText:string, Creditline:string, Classification:string, ImageUrl:string)  
//clear any previously ingested data if such exists
.clear table NationalGalleryArt data
.ingest into table NationalGalleryArt ('https://kustodetectiveagency.blob.core.windows.net/kda2c6art/artview.csv')
```

Okey, what's up with the instructions that we got... Look's like we need to decode something, but what's that for format?

Looking at the first hint from the case we can see that the code is a combination of two numbers separated by a slash. The first number looks like it's the the ObjectId and the second number could be anything. After some investigation I found that this could be a Book Cipher where the first number is our ObjectId and the second number is our word from another column. Let's see if we can find the word from the second number in the ProvenanceText column or another column for that matter.

> Can you find 41701/11 this word is located in the 131736/0 data?

So we need to fill in the words above. We start of by seeing that the ObjectId 41701 is in the table and we can see that the word is located in the ProvenanceText column.

```kusto
NationalGalleryArt
| where ObjectId == 41701
```

![](/img/Case6/Art_ObjId41701.png)

So what's up with the number 11? I cannot see anything that have 11 and the only column that have a exceptionaly long text is the ProvenanceText column. So let's see if we can find the word in the ProvenanceText column. Yes, I start of by counting manually... So counting to 11 and looking at the ProvenceText column we can see that the word is "where".

> Can you find *where* this word is located in the 131736/0 data?

Looks good so far! 

```kusto
NationalGalleryArt
| where ObjectId in (41701,131736)
```

![](/img/Case6/Art_ObjId41701_131736.png)

And for our last word in the first hint we get 'Museum'. 

> Can you find *where* this word is located in the *Museum* data?

Sounds like we have found the words!

We need to train to get this right because we have a loooong text to decode. Time to get to know **mv-expand** (Multi-value expand) and **with_itemindex**. Docs for [mv-expand operator](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator) and [with_itemindex](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator#using-with_itemindex). As of today maybe not so clear to use so I will try to explain it here.

In below query we gather our ObjectId's with the [**in()**](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator) operator and then we project the **ObjectId**, **Word** comes from *extract_all* where we will get all words in the ProvenanceText column and **ProvenanceText** columns (just to make sure we are getting it right). Then we use mv-expand to expand the Word column and we use with_itemindex to get the index of the WordToFind column. We then project the CodeToCrack, WordToFind and ProvenanceText columns. The CodeToCrack column is a combination of ObjectId and Index when we use **strcat()** (basically a string concatenation). 

The RegExp **@'(\w+)'** will get all words in the ProvenanceText column. 

* **\w** means that we matches any word character, equivalent to [a-zA-Z0-9_]
* **+** means that we matches between one and unlimited times, as many times as possible, giving back as needed (greedy)

```kusto
NationalGalleryArt
| where ObjectId in (41701,131736)
| project ObjectId, WordToFind = extract_all(@'(\w+)', ProvenanceText), ProvenanceText
| mv-expand with_itemindex=Index WordToFind
| project CodeToCrack=strcat(ObjectId,'/',Index), WordToFind, ProvenanceText
```

![](/img/Case6/Art_ObjId41701_131736_mvexpand.png)

Let's make it more interesting. We'll take the first hint and add it as an table with **.set-or-append**. We then join the two tables with **join** and **on**. We then project the **CodeHint** and **ProvenanceText** columns. 

```kusto
.set-or-append Case6Hint1 <|
print CodeHint = '41701/11 131736/0'
```

```kusto
Case6Hint1
| extend CodeToCrack = extract_all(@"(\d+/\d+)", CodeHint)
| mv-expand CodeToCrack to typeof(string)
| join kind = leftouter 
(
    NationalGalleryArt
    | project ObjectId, WordToFind = extract_all(@'(\w+)', ProvenanceText)
    | mv-expand with_itemindex=Index WordToFind
    | project CodeToCrack=strcat(ObjectId,'/',Index), WordToFind
) on CodeToCrack
| project CodeToCrack, WordToFind, CodeHint
```

What about the extract_all operator? You see now we want to extract the digits and the slash. The RegExp **@'(\d+/\d+)'** will get all digits and the slash in the CodeHint column.

* \d+ means that we matches a digit (equal to [0-9])
    * (+) matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)
* / matches the character / literally (case sensitive)
* \d+ means that we matches a digit (equal to [0-9])
    * (+) matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)

We continue with the **mv-expand** operator and now with the **to** operator. The **to** operator will convert the CodeToCrack column to a string. Then going for the **join** operator so we can more easily join the two tables (you see why we wanted to add the hint as a table?). The **leftouter** kind of the [join operator](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/joinoperator?pivots=azuredataexplorer&WT.mc_id=AZ-MVP-5004683) will return all rows from the left table, and the matched rows from the right table. The **on** operator will join the two tables on the CodeToCrack column. 

![](/img/Case6/Art_Hint1_Solved.png)

I do belive we can decode the instructions that we secretly got. Start by adding the instructions as a table with **.set-or-append**. Then we use the **print** operator to print the CodeToCrack column. 

```kusto
.set-or-append SecretInstructions <|
print CodeToCrack = 
```12204/497 62295/24 50883/678 47108/107 193867/3,
45534/141 hidden 100922/183 143461/1 1181/505 46187/380.
41526/155 66447/199 30241/114, 33745/154 12145/387 46437/398 177191/131:
293/64 41629/1506 210038/432, 41612/803 216839/1.

404/258 rules 40/186 1472/222 122894/2 46081/105:
41594/650 32579/439 44625/141 184121/19 33254/348 357/273 32589/821,
46171/687 punctuations 62420/10 50509/48 1447/128,
176565/82'56721/591 561/225 insensitive, 30744/129 76197/32.

1319/42 41599/216 68/457 136016/146, 42420/126'46198/389 42429/158 40091/108 41667/252,
1515/555 177593/223 176924/73 45889/65 159836/96 35080/384 32578/199.
1607/167 124996/9 71/56, 1303/187 45640/1114 72328/247 75802/11,
1168/146 163380/12 57541/116 206122/738 365/267 46026/211 46127/19.

119295/425 45062/128 12198/133 163917/238 45092/8 54183/4 42453/82:
561/433 9/387 37004/287 1493/118 41676/38 163917/238 3159/118 63264/687
1/905 1493/109 43723/252, 136355/1 1159/134 40062/172 32588/604,
158574/1 45411/8 10/892 127587/175 - 633/9 72328/247 1514/615 42940/138.

164958/84 221014/479 151526/7 111124/138, 41668/206 34109/46 1514/555,
147789/2 3228/152 993/323 166477/167 178042/167, 50753/91'207786/8 12/372.
1108/158'42423/150 12/309 66154/9 213566/11 44981/158 1197/300
40184/149 92994/63-71071/179 75093/7 211718/18 74211/5 46144/399.```
```

After we added the table we just change some of the KQL to get the right table and change the name of the column to Instruction. 

```kusto
SecretInstructions
| extend CodeToCrack = extract_all(@"(\d+/\d+)", Instruction)
| mv-expand CodeToCrack to typeof(string)
| join kind = leftouter 
(
    NationalGalleryArt
    | project ObjectId, WordToFind = extract_all(@'(\w+)', ProvenanceText)
    | mv-expand with_itemindex=Index WordToFind
    | project CodeToCrack=strcat(ObjectId,'/',Index), WordToFind
) on CodeToCrack
| project CodeToCrack, WordToFind, Instruction
```

Cool, we are getting closer and closer!

![](/img/Case6/Art_FirstCodeCrack.png)

Now we need to make it more easy to read, wonder if the summarize operator can help us? We can use the **make_list()** function to create a list of the WordToFind column. We then project that column. 

```kusto
SecretInstructions
| extend CodeToCrack = extract_all(@"(\d+/\d+)", Instruction)
| mv-expand CodeToCrack to typeof(string)
| join kind = leftouter 
(
    NationalGalleryArt
    | project ObjectId, WordToFind = extract_all(@'(\w+)', ProvenanceText)
    | mv-expand with_itemindex=Index WordToFind
    | project CodeToCrack=strcat(ObjectId,'/',Index), WordToFind
) on CodeToCrack
| summarize WordToFind = make_list(WordToFind) by Instruction
| project WordToFind
```

Okey, so not quite what we wanted. Now it's not in the order that we want. Wonder if replace_strings can help us? Observe that we want to try out replace_string**s** (plural) and not string. We do need to add a second **make_list** to get the CodeToCrack column. We then use the **replace_strings** function to replace the CodeToCrack column with the WordToFind column. We then project the entire secret message. 

* replace_strings: text,lookups,rewrite
    * text: The text to be rewritten, in this case our Instruction column.
    * lookups: A list of strings to be replaced, in this case our CodeToCrack column.
    * rewrite: A list of strings to replace the lookups with, in this case our WordToFind column.


```kusto
SecretInstructions
| extend CodeToCrack=extract_all(@"(\d+/\d+)", Instruction)
| mv-expand CodeToCrack to typeof(string)
| join kind = leftouter ( NationalGalleryArt
    | project ObjectId, WordToFind = extract_all(@'(\w+)', ProvenanceText)
    | mv-expand with_itemindex=Index WordToFind
    | project CodeToCrack=strcat(ObjectId,'/',Index), WordToFind
) on CodeToCrack
| project CodeToCrack, WordToFind, Instruction
| summarize FinalCodeToCrack=make_list(CodeToCrack), FinalWordToFind=make_list(WordToFind) by Instruction
| project SecretMsg = replace_strings(Instruction, FinalCodeToCrack, FinalWordToFind)
```

![](/img/Case6/Art_SecretMsg.png)

Yay! We got the secret message! 

```text
in catalogue of titles Grand,
three hidden words Demand your Hand.
when found all, they form A line:
A clear timeline, simply Fine.

words rules are simple to Review:
at least three Letters have in view,
all punctuations Mark the End,
they're case insensitive, my friend.

to find all words, you'll need some skill,
seeking the popular will guide you still.
below The King, the first word mounts,
the Second shares with Third their counts.

reveal the last word with Wise thought:
take first two letters from word most sought
into marked dozen, and change just one,
and with those two - the word is done.

so search the titles, high and low,
and when you find it, you'll know.
you've picked the Image that revealed
the pass-code to the World concealed.
```

(To be continued... Not finished yet)