libname mylib "C:\Users\wangjohn\Desktop\work\new";

 %let begindate= '01jan1980'd;
 %let testdate= '01jan1990'd;
 %let enddate= '31dec2010'd;

*利用s12数据，关联到wficn;
proc sql;
create table mydata as
select a.wficn, b.*
from mylib.link2 as a, mylib.s12 as b
where a.fundno=b.fundno and a.fdate=b.fdate and b.fdate<= &enddate and b.fdate>&begindate;******************;
quit;
proc sort data=mydata;by wficn fdate;run;

*对s12数据进行筛选IOC;
data mydata;
set mydata;
fyear=year(fdate);fmonth=month(fdate);
ryear=year(rdate);rmonth=month(rdate);
if ioc=5 or ioc=6 or ioc=8 then delete;
if wficn=. then delete;
drop ioc shrout2 prdate;
run;

*mfdb数据，关联到wficn;
proc sql;
create table mydata2 as
select a.wficn,b.*
from mylib.link1 as a, mylib.mfdb as b
where a.crsp_fundno=b.crsp_fundno and b.caldt<= &enddate;
quit;

proc sort data=mydata2;by wficn;run;

*根据wficn将总资产求和;
proc sql;
create table grpbywficn as
select wficn, caldt, sum(TNA_Latest) as sumtna
from mydata2
group by wficn, caldt;
quit;

data grpbywficn;
set grpbywficn;
*year=year(caldt);*month=month(caldt);
by wficn;lgdate=lag(caldt);
if first.wficn then lgdate= &begindate;
run;

proc sql;
create table linkdata as
select a.*,b.sumtna,b.caldt
from mydata as a, grpbywficn as b
where a.wficn=b.wficn and b.lgdate< a.rdate<=b.caldt; ******************;
quit;

*加上组合中持股数量;
proc sql;
create table tempstknum as
select wficn, rdate, count(cusip) as stknum
from linkdata
group by wficn, rdate;
create table linkdata2 as
select a.*, b.stknum
from linkdata as a, tempstknum as b
where a.wficn=b.wficn and a.rdate=b.rdate;
quit;

*加上股票价格和发行量信息;
data mylib.crsp;
set mylib.crsp;
year=year(date);
month=month(date);
run;

proc sort data=linkdata2; by wficn;run;
proc sql;
create table linkdata3 as
select a.*,b.prc as bprc, b.shrout
from linkdata2 as a, crsp as b
where a.cusip=b.cusip and a.ryear=b.year and a.rmonth=b.month;
quit;

proc sort data=linkdata3;by wficn fdate;run;

*follow Hartzmark,删小于20的,持股数大于发行量的,删除个股价值大于总价值的;
data linkdata4;
set linkdata3;
if stknum<20 then delete;
sumtna=sumtna*100;
shrout=shrout*1000;
tnaall=assets*10000;
holding=shares*prc;
if sumtna>0 then rate=assets/sumtna;
if rate>2 then delete;*****************;
if shares>shrout then delete;
if holding>tnaall then delete;
run;

data linkdata4;set linkdata4; drop lagrdate;run;
proc sort data=linkdata4;by wficn rdate;run;
*匹配rdate，看之前的持股。;
data rdatelink;set linkdata4 (keep=wficn rdate);run;
proc sort data=rdatelink nodupkey;by wficn rdate; run;
data rdatelink2;set rdatelink;
by wficn;lagrdate=lag(rdate);
if first.wficn then lagrdate=.;
run;

proc sql;
create table linkdata5 as
select a.*,b.rdate as fwrdate from linkdata4 as a,rdatelink2 as b
where a.wficn=b.wficn and a.rdate=b.lagrdate;
quit;

data temp3;set linkdata5 (keep= wficn rdate cusip prc shares tnaall);
rename rdate=fwrdate prc=fwprc shares=fwshares tnaall=fwtnaall;run;

proc sort data=temp3 nodupkey; by wficn fwrdate cusip; run;
proc sort data=linkdata5 nodupkey;by wficn fwrdate cusip; run;
data linkdata6;
merge linkdata5 (in=in1) temp3 (in=in2);
by wficn fwrdate cusip;
if in1;
change2=shares-fwshares;
run;

data temp4;
set linkdata6;
if fwshares=. or change2>0 then sell=1;
if sell=1;
keep wficn cusip rdate sell;
run;
proc sort data=temp4 nodupkey;by wficn rdate; run;
proc sql;
create table linkdata7 as
select a.*, b.sell as sellday from linkdata6 as a, temp4 as b 
where a.wficn=b.wficn and a.rdate=b.rdate;
quit;

data linkdata7;
set linkdata7;
drop fyear fmonth ryear rmonth bprc fwprc;
fwyear=year(fwrdate);
fwmonth=month(fwrdate);
run;

proc sql;
create table linkdata8 as
select a.*, b.prc as fwprc
from linkdata7 as a, crsp as b
where a.cusip=b.cusip and a.fwyear=b.year and a.fwmonth=b.month;
quit;

data linkgain;
set mylib.linkdata8;
gain=shares*(fwprc-prc);
run;

proc sort data=linkgain;
by wficn fwrdate descending gain;
run;

proc sql;
create table finallink as
select *, sum(gain) as ttgain
from linkgain
group by wficn, fwrdate;
quit;

data finallink;
set finallink;
if gain=. then delete;
if change2>0 or change2=. then sell=1;
else if change2<=0 then sell=0;
if ttgain>0 then tg=1;else if ttgain<=0 then tg=0;
by wficn fwrdate;
best=first.fwrdate;
worst=last.fwrdate;
gainpart=gain/ttgain;
run;

data best1;
set finallink;
if best=1;
data worst1;
set finallink;
if worst=1;
run;
proc sql;
create table gtl as
select a.*,b.gain as lossgain, b.sell as losssell from best1 as a, worst1 as b
where a.wficn=b.wficn and a.fwrdate=b.fwrdate and a.best=b.worst;
quit;

data keng;
set gtl;
cover=gain+lossgain;
run;
proc rank data=keng out=testrank groups=3;
var cover gain;
ranks num1 num2;
run;
proc summary data=testrank nway;
class  num1 num2 sell;
output out=fundnum(drop=_type_ rename=(_freq_=times));
run;
proc means noprint data=testrank;
class num1 num2;
var cover gain;
output out=ans mean(cover gain)=mc mg;
run;

proc reg data= outest=beta1 noprint;
model na = a dsa pa/noint;
by  Csrciccd2 year;
run;
