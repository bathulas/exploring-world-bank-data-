

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



    <div class="bk-root">
        <a href="http://bokeh.pydata.org" target="_blank" class="bk-logo bk-logo-small bk-logo-notebook"></a>
        <span id="14ed304e-7940-405c-8e25-af7116652922">Loading BokehJS ...</span>
    </div>







    <div class="bk-root">
        <div class="bk-plotdiv" id="d38f34fb-26b6-4416-916f-28d16ae46c1a"></div>
    </div>
<script type="text/javascript">
  
  (function(global) {
    function now() {
      return new Date();
    }
  
    var force = false;
  
    if (typeof (window._bokeh_onload_callbacks) === "undefined" || force === true) {
      window._bokeh_onload_callbacks = [];
      window._bokeh_is_loading = undefined;
    }
  
  
    
    if (typeof (window._bokeh_timeout) === "undefined" || force === true) {
      window._bokeh_timeout = Date.now() + 0;
      window._bokeh_failed_load = false;
    }
  
    var NB_LOAD_WARNING = {'data': {'text/html':
       "<div style='background-color: #fdd'>\n"+
       "<p>\n"+
       "BokehJS does not appear to have successfully loaded. If loading BokehJS from CDN, this \n"+
       "may be due to a slow or bad network connection. Possible fixes:\n"+
       "</p>\n"+
       "<ul>\n"+
       "<li>re-rerun `output_notebook()` to attempt to load from CDN again, or</li>\n"+
       "<li>use INLINE resources instead, as so:</li>\n"+
       "</ul>\n"+
       "<code>\n"+
       "from bokeh.resources import INLINE\n"+
       "output_notebook(resources=INLINE)\n"+
       "</code>\n"+
       "</div>"}};
  
    function display_loaded() {
      if (window.Bokeh !== undefined) {
        document.getElementById("d38f34fb-26b6-4416-916f-28d16ae46c1a").textContent = "BokehJS successfully loaded.";
      } else if (Date.now() < window._bokeh_timeout) {
        setTimeout(display_loaded, 100)
      }
    }if ((window.Jupyter !== undefined) && Jupyter.notebook.kernel) {
      comm_manager = Jupyter.notebook.kernel.comm_manager
      comm_manager.register_target("e005336f-2071-4dd1-b7f0-4a0d771217d6", function () {});
    }
  
    function run_callbacks() {
      window._bokeh_onload_callbacks.forEach(function(callback) { callback() });
      delete window._bokeh_onload_callbacks
      console.info("Bokeh: all callbacks have finished");
    }
  
    function load_libs(js_urls, callback) {
      window._bokeh_onload_callbacks.push(callback);
      if (window._bokeh_is_loading > 0) {
        console.log("Bokeh: BokehJS is being loaded, scheduling callback at", now());
        return null;
      }
      if (js_urls == null || js_urls.length === 0) {
        run_callbacks();
        return null;
      }
      console.log("Bokeh: BokehJS not loaded, scheduling load and callback at", now());
      window._bokeh_is_loading = js_urls.length;
      for (var i = 0; i < js_urls.length; i++) {
        var url = js_urls[i];
        var s = document.createElement('script');
        s.src = url;
        s.async = false;
        s.onreadystatechange = s.onload = function() {
          window._bokeh_is_loading--;
          if (window._bokeh_is_loading === 0) {
            console.log("Bokeh: all BokehJS libraries loaded");
            run_callbacks()
          }
        };
        s.onerror = function() {
          console.warn("failed to load library " + url);
        };
        console.log("Bokeh: injecting script tag for BokehJS library: ", url);
        document.getElementsByTagName("head")[0].appendChild(s);
      }
    };var element = document.getElementById("d38f34fb-26b6-4416-916f-28d16ae46c1a");
    if (element == null) {
      console.log("Bokeh: ERROR: autoload.js configured with elementid 'd38f34fb-26b6-4416-916f-28d16ae46c1a' but no matching script tag was found. ")
      return false;
    }
  
    var js_urls = [];
  
    var inline_js = [
      function(Bokeh) {
        (function() {
          var fn = function() {
            var docs_json = {"7455375e-3043-4d9d-9c60-97f8bdc47218":{"roots":{"references":[{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Sudan"}],"color":["#f22c40"],"country":["Sudan"],"fill_alpha":[0.8],"height":[83380000.0],"label":[{"country":"Sudan"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Sudan"],"y":[41690000.0]}},"id":"2167c69b-2fa5-45d0-80a5-ac295a19e17e","type":"ColumnDataSource"},{"attributes":{"data_source":{"id":"77ab2ea4-05cb-4e81-8814-8f96b5356597","type":"ColumnDataSource"},"glyph":{"id":"1122c509-f84d-4adc-879b-aff4c0bf95d0","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"14bc597e-23f2-4e4b-9212-34dd66af4fa4","type":"GlyphRenderer"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"52dcd792-66d7-462c-b163-ffbfece94306","type":"Rect"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"00f3c6ff-ef89-4d7c-8c3f-1d6900b4b49a","type":"Rect"},{"attributes":{"dimension":1,"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"},"ticker":{"id":"5661ab2e-0287-4a9c-88e2-37ccac0972d4","type":"BasicTicker"}},"id":"c9558bd6-a886-4801-8620-a23786486093","type":"Grid"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Nepal"}],"color":["#f22c40"],"country":["Nepal"],"fill_alpha":[0.8],"height":[87160000.0],"label":[{"country":"Nepal"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Nepal"],"y":[43580000.0]}},"id":"19ab3be0-2f60-4d76-8b4a-164fca3eff9f","type":"ColumnDataSource"},{"attributes":{"axis_label":"Country","formatter":{"id":"d622576b-8b79-4494-b8e9-0740625caa17","type":"CategoricalTickFormatter"},"major_label_orientation":0.7853981633974483,"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"},"ticker":{"id":"9b969dcb-c903-4dc4-88a7-35fb452e5a49","type":"CategoricalTicker"}},"id":"117b4f6c-7bb1-48b0-a182-248da96c015d","type":"CategoricalAxis"},{"attributes":{"data_source":{"id":"ff1bd5f4-692d-4e67-8a48-bd494d8c1553","type":"ColumnDataSource"},"glyph":{"id":"2b4881e0-1fee-49ce-a80e-59dc48f1758f","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"e4d02e7a-f3f8-43ac-8554-5770229c69b0","type":"GlyphRenderer"},{"attributes":{},"id":"9b969dcb-c903-4dc4-88a7-35fb452e5a49","type":"CategoricalTicker"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Ghana"}],"color":["#f22c40"],"country":["Ghana"],"fill_alpha":[0.8],"height":[75500000.0],"label":[{"country":"Ghana"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Ghana"],"y":[37750000.0]}},"id":"c52b70d2-5764-47c7-bf0a-bcf042f0fd17","type":"ColumnDataSource"},{"attributes":{"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"4d9aa947-61a9-4a00-ae2c-fb7d9298aa40","type":"HelpTool"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"699ab820-f033-4da6-86e9-c91fc5781ab4","type":"Rect"},{"attributes":{"format":"($ 0 a)"},"id":"ed928d7f-94ab-4864-8517-2e89398ac5a2","type":"NumeralTickFormatter"},{"attributes":{"data_source":{"id":"c5fad15c-7db1-4091-914a-ee0a7682ffcf","type":"ColumnDataSource"},"glyph":{"id":"cadee0be-c2e4-481b-abb5-7fcd14e495ec","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"da87c62f-d720-42dc-8b9d-3348159ef061","type":"GlyphRenderer"},{"attributes":{"data_source":{"id":"d27f7700-010e-4276-a965-d840b7f4307e","type":"ColumnDataSource"},"glyph":{"id":"044c2d27-67ab-4d89-b874-659981dcb8ac","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"71c75763-6b48-4399-9012-5366f8a20d27","type":"GlyphRenderer"},{"attributes":{"axis_label":"Sum( Grant_Amt )","formatter":{"id":"ed928d7f-94ab-4864-8517-2e89398ac5a2","type":"NumeralTickFormatter"},"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"},"ticker":{"id":"5661ab2e-0287-4a9c-88e2-37ccac0972d4","type":"BasicTicker"}},"id":"41db768d-5568-4994-b201-ee6a14e3073d","type":"LinearAxis"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"West Bank and Gaza"}],"color":["#f22c40"],"country":["West Bank and Gaza"],"fill_alpha":[0.8],"height":[75050000.0],"label":[{"country":"West Bank and Gaza"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["West Bank and Gaza"],"y":[37525000.0]}},"id":"392dba77-124e-4171-9e2c-c5eb6a31b081","type":"ColumnDataSource"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Madagascar"}],"color":["#f22c40"],"country":["Madagascar"],"fill_alpha":[0.8],"height":[85400000.0],"label":[{"country":"Madagascar"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Madagascar"],"y":[42700000.0]}},"id":"77ab2ea4-05cb-4e81-8814-8f96b5356597","type":"ColumnDataSource"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"8c3ff04b-c9b1-4150-9184-7a5df9ec649d","type":"Rect"},{"attributes":{"data_source":{"id":"19ab3be0-2f60-4d76-8b4a-164fca3eff9f","type":"ColumnDataSource"},"glyph":{"id":"00f3c6ff-ef89-4d7c-8c3f-1d6900b4b49a","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"a5bb923a-67d6-4fe8-aa82-fecb60ae9019","type":"GlyphRenderer"},{"attributes":{"data_source":{"id":"ca60c140-d1ef-41c7-a0cf-497c7dc2ddd4","type":"ColumnDataSource"},"glyph":{"id":"840e054c-5e52-4407-8fbf-f7029853d9d8","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"6fb78571-2c65-4263-92a1-51f0bc3843f5","type":"GlyphRenderer"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"044c2d27-67ab-4d89-b874-659981dcb8ac","type":"Rect"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"48838c4c-88d6-4ab8-9623-2f4fbe0b4947","type":"Rect"},{"attributes":{"data_source":{"id":"2167c69b-2fa5-45d0-80a5-ac295a19e17e","type":"ColumnDataSource"},"glyph":{"id":"8c3ff04b-c9b1-4150-9184-7a5df9ec649d","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"5de1a0de-5ef4-487f-8ceb-0ed7746d5b11","type":"GlyphRenderer"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Afghanistan"}],"color":["#f22c40"],"country":["Afghanistan"],"fill_alpha":[0.8],"height":[174730000.0],"label":[{"country":"Afghanistan"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Afghanistan"],"y":[87365000.0]}},"id":"e2e65b88-5303-4984-8ac4-b2b54d2a0978","type":"ColumnDataSource"},{"attributes":{"callback":null,"end":448150500.0},"id":"3b8bfb67-f0d0-433d-a106-edead93f530c","type":"Range1d"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"840e054c-5e52-4407-8fbf-f7029853d9d8","type":"Rect"},{"attributes":{"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"477d33b2-5167-4c3a-bc71-ec7ad02f89a3","type":"SaveTool"},{"attributes":{},"id":"5661ab2e-0287-4a9c-88e2-37ccac0972d4","type":"BasicTicker"},{"attributes":{"data_source":{"id":"e2e65b88-5303-4984-8ac4-b2b54d2a0978","type":"ColumnDataSource"},"glyph":{"id":"48838c4c-88d6-4ab8-9623-2f4fbe0b4947","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"a4ef62c5-dffd-42c9-95b1-2128126f4adf","type":"GlyphRenderer"},{"attributes":{"overlay":{"id":"8e56a010-4dde-42b1-876a-4874ce711a2a","type":"BoxAnnotation"},"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"ba168b0a-4386-4cbd-9d5f-284c000ef1d5","type":"BoxZoomTool"},{"attributes":{"plot":null,"text":"Top 10 Countries funded by the World Bank "},"id":"39d2be32-029c-4345-ba63-a02dfc3a5b2a","type":"Title"},{"attributes":{"active_drag":"auto","active_scroll":"auto","active_tap":"auto","tools":[{"id":"705e7f19-b1d4-4aa2-917e-b6170b1c9c60","type":"PanTool"},{"id":"76f99d43-3ab9-48e7-ba06-aa16de1d31d5","type":"WheelZoomTool"},{"id":"ba168b0a-4386-4cbd-9d5f-284c000ef1d5","type":"BoxZoomTool"},{"id":"477d33b2-5167-4c3a-bc71-ec7ad02f89a3","type":"SaveTool"},{"id":"a41d63cd-0e82-4d05-8a43-69f7baf3ca01","type":"ResetTool"},{"id":"4d9aa947-61a9-4a00-ae2c-fb7d9298aa40","type":"HelpTool"}]},"id":"c269251b-79dc-4516-9660-75a8c2f3db24","type":"Toolbar"},{"attributes":{"bottom_units":"screen","fill_alpha":{"value":0.5},"fill_color":{"value":"lightgrey"},"left_units":"screen","level":"overlay","line_alpha":{"value":1.0},"line_color":{"value":"black"},"line_dash":[4,4],"line_width":{"value":2},"plot":null,"render_mode":"css","right_units":"screen","top_units":"screen"},"id":"8e56a010-4dde-42b1-876a-4874ce711a2a","type":"BoxAnnotation"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"2b4881e0-1fee-49ce-a80e-59dc48f1758f","type":"Rect"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"cadee0be-c2e4-481b-abb5-7fcd14e495ec","type":"Rect"},{"attributes":{"below":[{"id":"117b4f6c-7bb1-48b0-a182-248da96c015d","type":"CategoricalAxis"}],"css_classes":null,"left":[{"id":"41db768d-5568-4994-b201-ee6a14e3073d","type":"LinearAxis"}],"renderers":[{"id":"8e56a010-4dde-42b1-876a-4874ce711a2a","type":"BoxAnnotation"},{"id":"6fb78571-2c65-4263-92a1-51f0bc3843f5","type":"GlyphRenderer"},{"id":"a4ef62c5-dffd-42c9-95b1-2128126f4adf","type":"GlyphRenderer"},{"id":"e4d02e7a-f3f8-43ac-8554-5770229c69b0","type":"GlyphRenderer"},{"id":"71c75763-6b48-4399-9012-5366f8a20d27","type":"GlyphRenderer"},{"id":"da87c62f-d720-42dc-8b9d-3348159ef061","type":"GlyphRenderer"},{"id":"a5bb923a-67d6-4fe8-aa82-fecb60ae9019","type":"GlyphRenderer"},{"id":"14bc597e-23f2-4e4b-9212-34dd66af4fa4","type":"GlyphRenderer"},{"id":"5de1a0de-5ef4-487f-8ceb-0ed7746d5b11","type":"GlyphRenderer"},{"id":"b2963db4-4ccc-4248-acd8-3549b5794e8f","type":"GlyphRenderer"},{"id":"f8572848-329c-4427-a83f-5fc5a5440cbf","type":"GlyphRenderer"},{"id":"117b4f6c-7bb1-48b0-a182-248da96c015d","type":"CategoricalAxis"},{"id":"41db768d-5568-4994-b201-ee6a14e3073d","type":"LinearAxis"},{"id":"c9558bd6-a886-4801-8620-a23786486093","type":"Grid"}],"title":{"id":"39d2be32-029c-4345-ba63-a02dfc3a5b2a","type":"Title"},"tool_events":{"id":"7ba34ef9-b02b-4ada-ae51-740a8c59fd22","type":"ToolEvents"},"toolbar":{"id":"c269251b-79dc-4516-9660-75a8c2f3db24","type":"Toolbar"},"x_mapper_type":"auto","x_range":{"id":"0197e081-8380-4842-b537-6d6992b3a6dc","type":"FactorRange"},"y_mapper_type":"auto","y_range":{"id":"3b8bfb67-f0d0-433d-a106-edead93f530c","type":"Range1d"}},"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"},{"attributes":{},"id":"d622576b-8b79-4494-b8e9-0740625caa17","type":"CategoricalTickFormatter"},{"attributes":{"data_source":{"id":"392dba77-124e-4171-9e2c-c5eb6a31b081","type":"ColumnDataSource"},"glyph":{"id":"699ab820-f033-4da6-86e9-c91fc5781ab4","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"f8572848-329c-4427-a83f-5fc5a5440cbf","type":"GlyphRenderer"},{"attributes":{"fill_alpha":{"field":"fill_alpha"},"fill_color":{"field":"color"},"height":{"field":"height","units":"data"},"line_color":{"field":"line_color"},"width":{"field":"width","units":"data"},"x":{"field":"x"},"y":{"field":"y"}},"id":"1122c509-f84d-4adc-879b-aff4c0bf95d0","type":"Rect"},{"attributes":{"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"705e7f19-b1d4-4aa2-917e-b6170b1c9c60","type":"PanTool"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Vietnam"}],"color":["#f22c40"],"country":["Vietnam"],"fill_alpha":[0.8],"height":[118240000.0],"label":[{"country":"Vietnam"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Vietnam"],"y":[59120000.0]}},"id":"ff1bd5f4-692d-4e67-8a48-bd494d8c1553","type":"ColumnDataSource"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"China"}],"color":["#f22c40"],"country":["China"],"fill_alpha":[0.8],"height":[426810000.0],"label":[{"country":"China"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["China"],"y":[213405000.0]}},"id":"ca60c140-d1ef-41c7-a0cf-497c7dc2ddd4","type":"ColumnDataSource"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Congo, Democratic Republic of"}],"color":["#f22c40"],"country":["Congo, Democratic Republic of"],"fill_alpha":[0.8],"height":[100500000.0],"label":[{"country":"Congo, Democratic Republic of"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Congo, Democratic Republic of"],"y":[50250000.0]}},"id":"d27f7700-010e-4276-a965-d840b7f4307e","type":"ColumnDataSource"},{"attributes":{"data_source":{"id":"c52b70d2-5764-47c7-bf0a-bcf042f0fd17","type":"ColumnDataSource"},"glyph":{"id":"52dcd792-66d7-462c-b163-ffbfece94306","type":"Rect"},"hover_glyph":null,"nonselection_glyph":null,"selection_glyph":null},"id":"b2963db4-4ccc-4248-acd8-3549b5794e8f","type":"GlyphRenderer"},{"attributes":{},"id":"7ba34ef9-b02b-4ada-ae51-740a8c59fd22","type":"ToolEvents"},{"attributes":{"callback":null,"factors":["China","Afghanistan","Vietnam","Congo, Democratic Republic of","Morocco","Nepal","Madagascar","Sudan","Ghana","West Bank and Gaza"]},"id":"0197e081-8380-4842-b537-6d6992b3a6dc","type":"FactorRange"},{"attributes":{"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"a41d63cd-0e82-4d05-8a43-69f7baf3ca01","type":"ResetTool"},{"attributes":{"callback":null,"column_names":["line_color","line_alpha","color","fill_alpha","height","width","y","x","label"],"data":{"chart_index":[{"country":"Morocco"}],"color":["#f22c40"],"country":["Morocco"],"fill_alpha":[0.8],"height":[91250000.0],"label":[{"country":"Morocco"}],"line_alpha":[1.0],"line_color":["white"],"width":[0.8],"x":["Morocco"],"y":[45625000.0]}},"id":"c5fad15c-7db1-4091-914a-ee0a7682ffcf","type":"ColumnDataSource"},{"attributes":{"plot":{"id":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","subtype":"Chart","type":"Plot"}},"id":"76f99d43-3ab9-48e7-ba06-aa16de1d31d5","type":"WheelZoomTool"}],"root_ids":["e9af399d-2d85-48be-b90e-c7ff23bf4cb1"]},"title":"Bokeh Application","version":"0.12.4"}};
            var render_items = [{"docid":"7455375e-3043-4d9d-9c60-97f8bdc47218","elementid":"d38f34fb-26b6-4416-916f-28d16ae46c1a","modelid":"e9af399d-2d85-48be-b90e-c7ff23bf4cb1","notebook_comms_target":"e005336f-2071-4dd1-b7f0-4a0d771217d6"}];
            
            Bokeh.embed.embed_items(docs_json, render_items);
          };
          if (document.readyState != "loading") fn();
          else document.addEventListener("DOMContentLoaded", fn);
        })();
      },
      function(Bokeh) {
      }
    ];
  
    function run_inline_js() {
      
      if ((window.Bokeh !== undefined) || (force === true)) {
        for (var i = 0; i < inline_js.length; i++) {
          inline_js[i](window.Bokeh);
        }if (force === true) {
          display_loaded();
        }} else if (Date.now() < window._bokeh_timeout) {
        setTimeout(run_inline_js, 100);
      } else if (!window._bokeh_failed_load) {
        console.log("Bokeh: BokehJS failed to load within specified timeout.");
        window._bokeh_failed_load = true;
      } else if (force !== true) {
        var cell = $(document.getElementById("d38f34fb-26b6-4416-916f-28d16ae46c1a")).parents('.cell').data().cell;
        cell.output_area.append_execute_result(NB_LOAD_WARNING)
      }
  
    }
  
    if (window._bokeh_is_loading === 0) {
      console.log("Bokeh: BokehJS loaded, going straight to plotting");
      run_inline_js();
    } else {
      load_libs(js_urls, function() {
        console.log("Bokeh: BokehJS plotting callback run at", now());
        run_inline_js();
      });
    }
  }(this));
</script>


    WARNING:/home/smauggy/anaconda2/lib/python2.7/site-packages/bokeh/core/validation/check.pyc:W-1003 (MALFORMED_CATEGORY_LABEL): Category labels cannot contain colons: [range:x_range] [first_value: 2012-11-22T00:00:00Z] [renderer: Chart(id='802c27ba-4af8-463f-8941-9fdab28eb089', ...)]





<p><code>&lt;Bokeh Notebook handle for <strong>In[46]</strong>&gt;</code></p>




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


```python

```
