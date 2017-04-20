

```python
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
```


```python
sc = SparkContext()

#creating a sparkcontext instance
```


```python
sqlContext = SQLContext(sc)

#creating a sqlcontext instance 
```


```python
data = sqlContext.read.json("world_bank.json")

#reading world bank data and creating a DataFrame


```


```python
data.printSchema()

#exploring the data schema
```

    root
     |-- _id: struct (nullable = true)
     |    |-- $oid: string (nullable = true)
     |-- approvalfy: string (nullable = true)
     |-- board_approval_month: string (nullable = true)
     |-- boardapprovaldate: string (nullable = true)
     |-- borrower: string (nullable = true)
     |-- closingdate: string (nullable = true)
     |-- country_namecode: string (nullable = true)
     |-- countrycode: string (nullable = true)
     |-- countryname: string (nullable = true)
     |-- countryshortname: string (nullable = true)
     |-- docty: string (nullable = true)
     |-- envassesmentcategorycode: string (nullable = true)
     |-- grantamt: long (nullable = true)
     |-- ibrdcommamt: long (nullable = true)
     |-- id: string (nullable = true)
     |-- idacommamt: long (nullable = true)
     |-- impagency: string (nullable = true)
     |-- lendinginstr: string (nullable = true)
     |-- lendinginstrtype: string (nullable = true)
     |-- lendprojectcost: long (nullable = true)
     |-- majorsector_percent: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- Name: string (nullable = true)
     |    |    |-- Percent: long (nullable = true)
     |-- mjsector_namecode: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- code: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |-- mjtheme: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- mjtheme_namecode: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- code: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |-- mjthemecode: string (nullable = true)
     |-- prodline: string (nullable = true)
     |-- prodlinetext: string (nullable = true)
     |-- productlinetype: string (nullable = true)
     |-- project_abstract: struct (nullable = true)
     |    |-- cdata: string (nullable = true)
     |-- project_name: string (nullable = true)
     |-- projectdocs: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- DocDate: string (nullable = true)
     |    |    |-- DocType: string (nullable = true)
     |    |    |-- DocTypeDesc: string (nullable = true)
     |    |    |-- DocURL: string (nullable = true)
     |    |    |-- EntityID: string (nullable = true)
     |-- projectfinancialtype: string (nullable = true)
     |-- projectstatusdisplay: string (nullable = true)
     |-- regionname: string (nullable = true)
     |-- sector: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- Name: string (nullable = true)
     |-- sector1: struct (nullable = true)
     |    |-- Name: string (nullable = true)
     |    |-- Percent: long (nullable = true)
     |-- sector2: struct (nullable = true)
     |    |-- Name: string (nullable = true)
     |    |-- Percent: long (nullable = true)
     |-- sector3: struct (nullable = true)
     |    |-- Name: string (nullable = true)
     |    |-- Percent: long (nullable = true)
     |-- sector4: struct (nullable = true)
     |    |-- Name: string (nullable = true)
     |    |-- Percent: long (nullable = true)
     |-- sector_namecode: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- code: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |-- sectorcode: string (nullable = true)
     |-- source: string (nullable = true)
     |-- status: string (nullable = true)
     |-- supplementprojectflg: string (nullable = true)
     |-- theme1: struct (nullable = true)
     |    |-- Name: string (nullable = true)
     |    |-- Percent: long (nullable = true)
     |-- theme_namecode: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- code: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |-- themecode: string (nullable = true)
     |-- totalamt: long (nullable = true)
     |-- totalcommamt: long (nullable = true)
     |-- url: string (nullable = true)
    



```python
data.select(data['countryshortname'],data['grantamt'],data['status']).groupBy('status').count().show()

#number of active and closed grants
```

    +------+-----+
    |status|count|
    +------+-----+
    |Active|  438|
    |Closed|   62|
    +------+-----+
    



```python
data.select(data['countryshortname'],data['grantamt'],data['status']).orderBy(desc('grantamt')).show()
#grant amount by country and their status
```

    +--------------------+---------+------+
    |    countryshortname| grantamt|status|
    +--------------------+---------+------+
    |               China|365000000|Active|
    |         Afghanistan|100000000|Active|
    |Congo, Democratic...|100000000|Active|
    |          Madagascar| 85400000|Active|
    |             Vietnam| 84600000|Active|
    |               Sudan| 76500000|Active|
    |               Ghana| 75500000|Active|
    |         Afghanistan| 74730000|Active|
    |             Morocco| 70000000|Active|
    |           Nicaragua| 51900000|Active|
    |              Jordan| 50000000|Active|
    |               Nepal| 46500000|Active|
    |                Mali| 41700000|Active|
    |  West Bank and Gaza| 40000000|Closed|
    |              Zambia| 36000000|Active|
    |          Bangladesh| 33800000|Active|
    |           Indonesia| 31700000|Active|
    |               Nepal| 31000000|Active|
    |               China| 27280000|Active|
    |          Kazakhstan| 21760000|Active|
    +--------------------+---------+------+
    only showing top 20 rows
    



```python
data.select('borrower','countryshortname','regionname','grantamt','status').orderBy(desc('grantamt')).show()

#region wise borrowers and their borrowed amounts
```

    +--------------------+--------------------+--------------------+---------+------+
    |            borrower|    countryshortname|          regionname| grantamt|status|
    +--------------------+--------------------+--------------------+---------+------+
    |PEOPLE'S REPULIC ...|               China|East Asia and Pac...|365000000|Active|
    |ISLAMIC REPUBLIC ...|         Afghanistan|          South Asia|100000000|Active|
    |MINISTERE DE L'ED...|Congo, Democratic...|              Africa|100000000|Active|
    |MINISTRY OF FINAN...|          Madagascar|              Africa| 85400000|Active|
    |SOCIALIST REPUBLI...|             Vietnam|East Asia and Pac...| 84600000|Active|
    | GOVERNMENT OF SUDAN|               Sudan|              Africa| 76500000|Active|
    |GHANA MINISTRY OF...|               Ghana|              Africa| 75500000|Active|
    |GOVERNMENT OF AFG...|         Afghanistan|          South Asia| 74730000|Active|
    |FONDS D'EQUIPMENT...|             Morocco|Middle East and N...| 70000000|Active|
    |REPUBLIC OF NICAR...|           Nicaragua|Latin America and...| 51900000|Active|
    |MINISTRY OF PLANN...|              Jordan|Middle East and N...| 50000000|Active|
    | GOVERNMENT OF NEPAL|               Nepal|          South Asia| 46500000|Active|
    |MINISTRY OF ECONO...|                Mali|              Africa| 41700000|Active|
    |PALESTINE LIBERAT...|  West Bank and Gaza|Middle East and N...| 40000000|Closed|
    | MINISTRY OF FINANCE|              Zambia|              Africa| 36000000|Active|
    |MINISTRY OF ENVIR...|          Bangladesh|          South Asia| 33800000|Active|
    |GOVERNMENT OF IND...|           Indonesia|East Asia and Pac...| 31700000|Active|
    | GOVERNMENT OF NEPAL|               Nepal|          South Asia| 31000000|Active|
    |PEOPLE'S REPUBLIC...|               China|East Asia and Pac...| 27280000|Active|
    |REPUBLIC OF KAZAK...|          Kazakhstan|Europe and Centra...| 21760000|Active|
    +--------------------+--------------------+--------------------+---------+------+
    only showing top 20 rows
    



```python
data.select('borrower','countryshortname','regionname','grantamt','status').groupBy('regionname').count().show()

#number of borrowers by region
```

    +--------------------+-----+
    |          regionname|count|
    +--------------------+-----+
    |          South Asia|   65|
    |Middle East and N...|   54|
    |              Africa|  152|
    |East Asia and Pac...|  100|
    |               Other|    2|
    |Europe and Centra...|   74|
    |Latin America and...|   53|
    +--------------------+-----+
    



```python
data.select('borrower','countryshortname','regionname','grantamt','status').groupBy('regionname').avg('grantamt').show()

#each region's avg grant amt
```

    +--------------------+------------------+
    |          regionname|     avg(grantamt)|
    +--------------------+------------------+
    |          South Asia| 6092461.538461538|
    |Middle East and N...| 5311296.296296297|
    |              Africa|4037039.4736842103|
    |East Asia and Pac...|         7000400.0|
    |               Other|         3060000.0|
    |Europe and Centra...|1695540.5405405406|
    |Latin America and...|1662641.5094339622|
    +--------------------+------------------+
    



```python
bank_rdd = data.select('boardapprovaldate','borrower','project_name','closingdate','projectstatusdisplay','totalamt','url','countrycode','countryshortname','regionname','grantamt','status').rdd

#creating a rdd from the dataframe
```


```python
bank_rdd.filter(lambda x: 'ET' in x[7]).collect()

#filter all rows that corresponds to Ethiopia
```




    [Row(boardapprovaldate=u'2013-11-12T00:00:00Z', borrower=u'FEDERAL DEMOCRATIC REPUBLIC OF ETHIOPIA', project_name=u'Ethiopia General Education Quality Improvement Project II', closingdate=u'2018-07-07T00:00:00Z', projectstatusdisplay=u'Active', totalamt=130000000, url=u'http://www.worldbank.org/projects/P129828/ethiopia-general-education-quality-improvement-project-ii?lang=en', countrycode=u'ET', countryshortname=u'Ethiopia', regionname=u'Africa', grantamt=0, status=u'Active'),
     Row(boardapprovaldate=u'2013-02-28T00:00:00Z', borrower=u'FEDERAL DEMOCRATIC REP. OF ETHIOPIA', project_name=u'Ethiopia Health MDG Support Operation', closingdate=u'2018-06-30T00:00:00Z', projectstatusdisplay=u'Active', totalamt=100000000, url=u'http://www.worldbank.org/projects/P123531/ethiopia-health-mdg-support-operation?lang=en', countrycode=u'ET', countryshortname=u'Ethiopia', regionname=u'Africa', grantamt=0, status=u'Active'),
     Row(boardapprovaldate=u'2012-09-25T00:00:00Z', borrower=u'FEDERAL DEMOCRATIC REPUBLIC OF ETHIOPIA', project_name=u'Ethiopia-Transport Sector Project in Support of RSDP4', closingdate=u'2019-04-30T00:00:00Z', projectstatusdisplay=u'Active', totalamt=415000000, url=u'http://www.worldbank.org/projects/P117731/ethiopia-transport-sector-project-support-rsdp4?lang=en', countrycode=u'ET', countryshortname=u'Ethiopia', regionname=u'Africa', grantamt=0, status=u'Active'),
     Row(boardapprovaldate=u'2012-09-25T00:00:00Z', borrower=u'FED DEM REPUBLIC OF ETHIOPIA', project_name=u'Ethiopia Promoting Basic Services Program Phase III Project', closingdate=u'2018-01-07T00:00:00Z', projectstatusdisplay=u'Active', totalamt=600000000, url=u'http://www.worldbank.org/projects/P128891/ethiopia-protection-basic-services-program-phase-iii-project?lang=en', countrycode=u'ET', countryshortname=u'Ethiopia', regionname=u'Africa', grantamt=0, status=u'Active')]




```python
pairs = bank_rdd.map(lambda x: (x[8],x[10]))
pairs1 = bank_rdd.map(lambda x: (x[0],x[10]))


#creating pipelinedRDD transformations to select country, grant_amt and boardapprovaldata,grant_amts respectively
```


```python
import pandas as pd
from operator import add


df = pd.DataFrame(pairs.reduceByKey(add).collect())

#collecting all the countries by reducing on the countryshortname key while adding their grant amounts

df.columns = ['country','grant_amt']

#give names to the columns
```


```python
sorted_data = df.sort_values('grant_amt',ascending=False)
#sort the dataframe on grant_amt in descending order
```


```python
from bokeh.charts import Bar, show
from bokeh.charts.attributes import ColorAttr, CatAttr
from bokeh.models import NumeralTickFormatter
from bokeh.io import output_notebook
output_notebook()
p = Bar(sorted_data.head(10), values='grant_amt', title="Top 10 Countries funded by the World Bank ", label=CatAttr(columns=['country'], sort=False),legend=False)
p.yaxis.formatter=NumeralTickFormatter(format="($ 0 a)")
show(p, notebook_handle=True)

#creating a bokeh visualization to show the top 10 grant receiving countries 
```


```python
from bokeh.charts import TimeSeries
from bokeh.models import DatetimeTickFormatter
output_notebook()


timedata = pd.DataFrame(pairs1.reduceByKey(add).collect())

timedata.columns = ['boardapprovaldate','grant_amt']

timedata['boardapprovaldate'] = pd.to_datetime(timedata['boardapprovaldate'], format=('%Y-%m-%dT%H:%M:%SZ'))

p = TimeSeries(timedata, x='boardapprovaldate',y='grant_amt')

p.yaxis.formatter=NumeralTickFormatter(format="($ 0 a)")
p.xaxis.formatter=DatetimeTickFormatter(
        days=["%d %B %Y"],
        months=["%d %B %Y"],
        years=["%d %B %Y"])


show(p)

```
