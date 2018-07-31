package com.example.etl.services;

import com.example.etl.utils.DateTimeUtils;
import com.example.etl.utils.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by Administrator on 2018/2/6.
 */
@Service
public class ExportToExcel {
    String date = DateTimeUtils.getdateminone();
    String month =DateTimeUtils.getmonthminone();
    String nextmonth = DateTimeUtils.getmonthaddone();
    String nexttowmonth = DateTimeUtils.getmonthaddtow();
    int monthminonenum = DateTimeUtils.getmonthminonenum();
    String todayminone = DateTimeUtils.getdateminone();
    String lastweek = DateTimeUtils.getlastweek();


    //报案
    public void  baoan1(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("MAKECOM");
        headData.put("MAKECOM","业务机构");

        keywords.add("licenseno");
        headData.put("licenseno","车牌");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("REPORTORNAME");
        headData.put("REPORTORNAME","报案人");

        keywords.add("REPORTORNUMBER");
        headData.put("REPORTORNUMBER","报案电话");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEHOUR");
        headData.put("DAMAGEHOUR","出险时间");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("DAMAGENAME");
        headData.put("DAMAGENAME","出险原因");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/通赔标识");


        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("truncate(b.SUMESTIMATEFEE,2)");
        headData.put("truncate(b.SUMESTIMATEFEE,2)","估损金额");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handlername");
        headData.put("handlername","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("handler1name");
        headData.put("handler1name","归属人");

        keywords.add("firstsiteflag");
        headData.put("firstsiteflag","是否第一现场");

        File file = new File(FileUtils.getTitle2("报案清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }
    public void  baoan2(List<Map<String, Object>>datas,String area){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("MAKECOM");
        headData.put("MAKECOM","业务机构");

        keywords.add("licenseno");
        headData.put("licenseno","车牌");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("REPORTORNAME");
        headData.put("REPORTORNAME","报案人");

        keywords.add("REPORTORNUMBER");
        headData.put("REPORTORNUMBER","报案电话");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEHOUR");
        headData.put("DAMAGEHOUR","出险时间");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("DAMAGENAME");
        headData.put("DAMAGENAME","出险原因");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/通赔标识");


        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("truncate(b.SUMESTIMATEFEE,2)");
        headData.put("truncate(b.SUMESTIMATEFEE,2)","估损金额");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handlername");
        headData.put("handlername","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("handler1name");
        headData.put("handler1name","归属人");

        keywords.add("firstsiteflag");
        headData.put("firstsiteflag","是否第一现场");

        File file = new File(FileUtils.getTitle2(area+"-"+monthminonenum+"月报案清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }
    //送修
    public void  songxiu1(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("COMCODE");
        headData.put("COMCODE","归属机构");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("CLAIMNO");
        headData.put("CLAIMNO","立案号");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/赔标识");

        keywords.add("LICENSENO");
        headData.put("LICENSENO","车牌");

        keywords.add("BRANDNAME");
        headData.put("BRANDNAME","车型名称");

        keywords.add("POLICYNO");
        headData.put("POLICYNO","保单号");

        keywords.add("INSUREDNAME");
        headData.put("INSUREDNAME","被保险人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道");

        keywords.add("STARTDATE");
        headData.put("STARTDATE","起保日期");

        keywords.add("ENDDATE");
        headData.put("ENDDATE","终保日期");

        keywords.add("BUSINESSNATURE");
        headData.put("BUSINESSNATURE","业务来源");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");

        keywords.add("REPAIRFACTORYCODE");
        headData.put("REPAIRFACTORYCODE","修理厂代码");

        keywords.add("REPAIRFACTORYNAME");
        headData.put("REPAIRFACTORYNAME","修理厂名称");

        keywords.add("HANDLERNAME");
        headData.put("HANDLERNAME","定损员");

        keywords.add("DEFLOSSDATE");
        headData.put("DEFLOSSDATE","定损日期");

        keywords.add("UNDERWRITENAME");
        headData.put("UNDERWRITENAME","核损员");

        keywords.add("truncate(a2.SUMVERILOSSFEE,2)");
        headData.put("truncate(a2.SUMVERILOSSFEE,2)","定损金额");

        keywords.add("truncate(a2.SUMLOSSFEE,2)");
        headData.put("truncate(a2.SUMLOSSFEE,2)","核损金额");

        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handname");
        headData.put("handname","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("hand1name");
        headData.put("hand1name","归属人");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("UNDERWRITEENDDATE");
        headData.put("UNDERWRITEENDDATE","核损完成时间");

        File file = new File(FileUtils.getTitle2("送修清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    public void  songxiu2(List<Map<String, Object>>datas,String area){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("COMCODE");
        headData.put("COMCODE","归属机构");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("CLAIMNO");
        headData.put("CLAIMNO","立案号");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/赔标识");

        keywords.add("LICENSENO");
        headData.put("LICENSENO","车牌");

        keywords.add("BRANDNAME");
        headData.put("BRANDNAME","车型名称");

        keywords.add("POLICYNO");
        headData.put("POLICYNO","保单号");

        keywords.add("INSUREDNAME");
        headData.put("INSUREDNAME","被保险人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道");

        keywords.add("STARTDATE");
        headData.put("STARTDATE","起保日期");

        keywords.add("ENDDATE");
        headData.put("ENDDATE","终保日期");

        keywords.add("BUSINESSNATURE");
        headData.put("BUSINESSNATURE","业务来源");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");

        keywords.add("REPAIRFACTORYCODE");
        headData.put("REPAIRFACTORYCODE","修理厂代码");

        keywords.add("REPAIRFACTORYNAME");
        headData.put("REPAIRFACTORYNAME","修理厂名称");

        keywords.add("HANDLERNAME");
        headData.put("HANDLERNAME","定损员");

        keywords.add("DEFLOSSDATE");
        headData.put("DEFLOSSDATE","定损日期");

        keywords.add("UNDERWRITENAME");
        headData.put("UNDERWRITENAME","核损员");

        keywords.add("truncate(a2.SUMVERILOSSFEE,2)");
        headData.put("truncate(a2.SUMVERILOSSFEE,2)","定损金额");

        keywords.add("truncate(a2.SUMLOSSFEE,2)");
        headData.put("truncate(a2.SUMLOSSFEE,2)","核损金额");

        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handname");
        headData.put("handname","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("hand1name");
        headData.put("hand1name","归属人");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("UNDERWRITEENDDATE");
        headData.put("UNDERWRITEENDDATE","核损完成时间");

        File file = new File(FileUtils.getTitle2(area+"-"+monthminonenum+"月送修清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //核损
    public void  hesun1(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("sumlossfee");
        headData.put("sumlossfee","核损合计金额");

        keywords.add("sumverilossfee");
        headData.put("sumverilossfee","修理项目金额");

        keywords.add("cetainlosstype");
        headData.put("cetainlosstype","定损方式");


        keywords.add("repairfactorytype");
        headData.put("repairfactorytype","修理厂类型");

        keywords.add("repairfactorytypename");
        headData.put("repairfactorytypename","修理厂类型名称");

        keywords.add("registno");
        headData.put("registno","报案号");

        keywords.add("lflag");
        headData.put("lflag","保单类型");


        keywords.add("policyno");
        headData.put("policyno","保单号");


        keywords.add("inputtime");
        headData.put("inputtime","输入日期");

        keywords.add("enddeflossdate");
        headData.put("enddeflossdate","最后定损日期");


        keywords.add("finalhandlername");
        headData.put("finalhandlername","定损输入人员");


        keywords.add("underwritecode");
        headData.put("underwritecode","核损人员代码");


        keywords.add("underwritename");
        headData.put("underwritename","核损人员");

        keywords.add("underwriteenddate");
        headData.put("underwriteenddate","核损通过日期");


        keywords.add("sumverichgcompfee");
        headData.put("sumverichgcompfee","换件项目金额");

        keywords.add("sumverirepairfee");
        headData.put("sumverirepairfee","维修换件项目金额");

        keywords.add("flag1");
        headData.put("flag1","是否核损");

        keywords.add("examfactorycode");
        headData.put("examfactorycode","拆检厂代码");

        keywords.add("examfactoryname");
        headData.put("examfactoryname","拆检厂名称");


        keywords.add("repairfactorycode");
        headData.put("repairfactorycode","修理厂代码");


        keywords.add("repairfactoryname");
        headData.put("repairfactoryname","修理厂名称");



        keywords.add("prplthirdpartid");
        headData.put("prplthirdpartid","对应车辆损失项表Id ");



        keywords.add("reclaim");
        headData.put("reclaim","是否生成旧件回收任务");


        keywords.add("reclaimcode");
        headData.put("reclaimcode","是否生成旧件回收任务码");

        keywords.add("reclaimname");
        headData.put("reclaimname","回收任务处理人");

        keywords.add("reclaiminputtime");
        headData.put("reclaiminputtime","回收任务开始时间");


        keywords.add("reclaimoutputtime");
        headData.put("reclaimoutputtime","回收任务完成时间");

        keywords.add("reclaimid");
        headData.put("reclaimid","回收任务id");

        keywords.add("lossmainid");
        headData.put("lossmainid","定损单主表Id ");


        keywords.add("count1");
        headData.put("count1","换件项目数1");

        keywords.add("count2");
        headData.put("count2","换件项目数2");


        keywords.add("brandname");
        headData.put("brandname","车型名称");

        keywords.add("flag3");
        headData.put("flag3","案件状态");

        keywords.add("flag4");
        headData.put("flag4","是否人伤");

        keywords.add("flag5");
        headData.put("flag5","事故类型");

        keywords.add("opname1");
        headData.put("opname1","理算经办人");

        keywords.add("opname2");
        headData.put("opname2","核赔人员");

        keywords.add("opname3");
        headData.put("opname3","单证收集人员");



        keywords.add("opname4");
        headData.put("opname4","调度人员");

        keywords.add("outdate1");
        headData.put("outdate1","调度流出时间");

        keywords.add("outdate2");
        headData.put("outdate2","理算流出时间");

        keywords.add("endcasedate");
        headData.put("endcasedate","结案日期");

        keywords.add("agentcode");
        headData.put("agentcode","业务渠道");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","业务员代码");

        keywords.add("insuredcode");
        headData.put("insuredcode","被保人代码");

        keywords.add("insuredname");
        headData.put("insuredname","被保人姓名");


        keywords.add("brandname1");
        headData.put("brandname1","车型名称1");


        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");


        keywords.add("damagedate");
        headData.put("damagedate","出险日期");


        keywords.add("reportdate");
        headData.put("reportdate","报案日期");


        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");


        keywords.add("monopolycode");
        headData.put("monopolycode","推修码");

        keywords.add("monopolyname");
        headData.put("monopolyname","推修厂");

        keywords.add("checknature");
        headData.put("checknature","是否现场查勘");

        keywords.add("checker1");
        headData.put("checker1","查勘员1");

        keywords.add("checker2");
        headData.put("checker2","查勘员2");

        keywords.add("licenseno0");
        headData.put("licenseno0","车牌号");

        keywords.add("brandname0");
        headData.put("brandname0","车型名称0");

        keywords.add("useyears0");
        headData.put("useyears0","使用年限");


        File file = new File(FileUtils.getTitle2("核损清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    public void  hesun2(List<Map<String, Object>>datas,String area){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("sumlossfee");
        headData.put("sumlossfee","核损合计金额");

        keywords.add("sumverilossfee");
        headData.put("sumverilossfee","修理项目金额");

        keywords.add("cetainlosstype");
        headData.put("cetainlosstype","定损方式");

        keywords.add("repairfactorytypename");
        headData.put("repairfactorytypename","修理厂类型");

        keywords.add("registno");
        headData.put("registno","报案号");

        keywords.add("lflag");
        headData.put("lflag","保单类型");

        keywords.add("licenseno0");
        headData.put("licenseno0","车牌号");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("insuredcode");
        headData.put("insuredcode","被保人代码");

        keywords.add("insuredname");
        headData.put("insuredname","被保人姓名");

        keywords.add("inputtime");
        headData.put("inputtime","输入日期");

        keywords.add("enddeflossdate");
        headData.put("enddeflossdate","最后定损日期");

        keywords.add("finalhandlername");
        headData.put("finalhandlername","定损输入人员");

        keywords.add("underwritename");
        headData.put("underwritename","核损人员代码");

        keywords.add("underwriteenddate");
        headData.put("underwriteenddate","核损通过日期");

        keywords.add("count1");
        headData.put("count1","换件项目数");

        keywords.add("sumverichgcompfee");
        headData.put("sumverichgcompfee","换件项目金额");

        keywords.add("flag1");
        headData.put("flag1","是否核损");

        keywords.add("examfactorycode");
        headData.put("examfactorycode","拆检厂代码");

        keywords.add("examfactoryname");
        headData.put("examfactoryname","拆检厂名称");

        keywords.add("repairfactorycode");
        headData.put("repairfactorycode","修理厂代码");

        keywords.add("repairfactoryname");
        headData.put("repairfactoryname","修理厂名称");

        keywords.add("brandname");
        headData.put("brandname","车型名称");

        keywords.add("brandname0");
        headData.put("brandname0","车型名称0");

        keywords.add("useyears0");
        headData.put("useyears0","使用年限");

        keywords.add("flag3");
        headData.put("flag3","案件状态");

        keywords.add("flag4");
        headData.put("flag4","是否人伤");

        keywords.add("flag5");
        headData.put("flag5","事故类型");

        keywords.add("opname1");
        headData.put("opname1","理算经办人");

        keywords.add("opname2");
        headData.put("opname2","核赔人员");

        keywords.add("opname3");
        headData.put("opname3","单证收集人员");

        keywords.add("id");
        headData.put("id","内部流水号");

        keywords.add("opname4");
        headData.put("opname4","调度人员");

        keywords.add("outdate1");
        headData.put("outdate1","调度流出时间");

        keywords.add("outdate2");
        headData.put("outdate2","理算流出时间");

        keywords.add("endcasedate");
        headData.put("endcasedate","结案日期");

        keywords.add("agentcode");
        headData.put("agentcode","业务渠道");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","业务员代码");

        keywords.add("brandname1");
        headData.put("brandname1","车型名称1");

        keywords.add("brandname");
        headData.put("brandname","车型名称");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("damagedate");
        headData.put("damagedate","出险日期");

        keywords.add("reportdate");
        headData.put("reportdate","报案日期");

        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");

        keywords.add("checknature");
        headData.put("checknature","是否现场查勘");

        keywords.add("checker1");
        headData.put("checker1","查勘员1");

        keywords.add("checker2");
        headData.put("checker2","查勘员2");

        keywords.add("monopolycode");
        headData.put("monopolycode","推修码");

        keywords.add("monopolyname");
        headData.put("monopolyname","推修厂");

        keywords.add("reclaim");
        headData.put("reclaim","是否生成旧件回收任务");

        keywords.add("reclaimcode");
        headData.put("reclaimcode","是否生成旧件回收任务码");

        keywords.add("reclaimname");
        headData.put("reclaimname","回收任务处理人");

        keywords.add("reclaiminputtime");
        headData.put("reclaiminputtime","reclaiminputtime");

        keywords.add("reclaimoutputtime");
        headData.put("reclaimoutputtime","回收任务完成时间");

        File file = new File(FileUtils.getTitle2(area+"-"+monthminonenum+"月核损清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //结案
    public void  jiean(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("COMCODE");
        headData.put("COMCODE","归属机构");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("CLAIMNO");
        headData.put("CLAIMNO","立案号");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/赔标识");

        keywords.add("LICENSENO");
        headData.put("LICENSENO","车牌");

        keywords.add("BRANDNAME");
        headData.put("BRANDNAME","车型名称");

        keywords.add("POLICYNO");
        headData.put("POLICYNO","保单号");

        keywords.add("INSUREDNAME");
        headData.put("INSUREDNAME","被保险人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道");

        keywords.add("STARTDATE");
        headData.put("STARTDATE","起保日期");

        keywords.add("ENDDATE");
        headData.put("ENDDATE","终保日期");

        keywords.add("BUSINESSNATURE");
        headData.put("BUSINESSNATURE","业务来源");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("endcasedate");
        headData.put("endcasedate","结案时间");

        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");

        keywords.add("REPAIRFACTORYCODE");
        headData.put("REPAIRFACTORYCODE","修理厂代码");

        keywords.add("REPAIRFACTORYNAME");
        headData.put("REPAIRFACTORYNAME","修理厂名称");

        keywords.add("HANDLERNAME");
        headData.put("HANDLERNAME","定损员");

        keywords.add("DEFLOSSDATE");
        headData.put("DEFLOSSDATE","定损日期");

        keywords.add("UNDERWRITENAME");
        headData.put("UNDERWRITENAME","核损员");

        keywords.add("truncate(a2.SUMVERILOSSFEE,2)");
        headData.put("truncate(a2.SUMVERILOSSFEE,2)","定损金额");

        keywords.add("truncate(a2.SUMLOSSFEE,2)");
        headData.put("truncate(a2.SUMLOSSFEE,2)","核损金额");

        keywords.add("sumpaid");
        headData.put("sumpaid","总赔付金额");

        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handname");
        headData.put("handname","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("hand1name");
        headData.put("hand1name","归属人");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("UNDERWRITEENDDATE");
        headData.put("UNDERWRITEENDDATE","核损完成时间");




        File file = new File(FileUtils.getTitle2("结案清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    public void  jiean2(List<Map<String, Object>>datas,String area){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("COMCODE");
        headData.put("COMCODE","归属机构");

        keywords.add("REGISTNO");
        headData.put("REGISTNO","报案号");

        keywords.add("CLAIMNO");
        headData.put("CLAIMNO","立案号");

        keywords.add("LFLAG");
        headData.put("LFLAG","自/赔标识");

        keywords.add("LICENSENO");
        headData.put("LICENSENO","车牌");

        keywords.add("BRANDNAME");
        headData.put("BRANDNAME","车型名称");

        keywords.add("POLICYNO");
        headData.put("POLICYNO","保单号");

        keywords.add("INSUREDNAME");
        headData.put("INSUREDNAME","被保险人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道");

        keywords.add("STARTDATE");
        headData.put("STARTDATE","起保日期");

        keywords.add("ENDDATE");
        headData.put("ENDDATE","终保日期");

        keywords.add("BUSINESSNATURE");
        headData.put("BUSINESSNATURE","业务来源");

        keywords.add("DAMAGEDATE");
        headData.put("DAMAGEDATE","出险日期");

        keywords.add("DAMAGEADDRESS");
        headData.put("DAMAGEADDRESS","出险地址");

        keywords.add("REPORTDATE");
        headData.put("REPORTDATE","报案日期");

        keywords.add("endcasedate");
        headData.put("endcasedate","结案时间");

        keywords.add("reportornumber");
        headData.put("reportornumber","报案电话");

        keywords.add("REPAIRFACTORYCODE");
        headData.put("REPAIRFACTORYCODE","修理厂代码");

        keywords.add("REPAIRFACTORYNAME");
        headData.put("REPAIRFACTORYNAME","修理厂名称");

        keywords.add("HANDLERNAME");
        headData.put("HANDLERNAME","定损员");

        keywords.add("DEFLOSSDATE");
        headData.put("DEFLOSSDATE","定损日期");

        keywords.add("UNDERWRITENAME");
        headData.put("UNDERWRITENAME","核损员");

        keywords.add("truncate(a2.SUMVERILOSSFEE,2)");
        headData.put("truncate(a2.SUMVERILOSSFEE,2)","定损金额");

        keywords.add("truncate(a2.SUMLOSSFEE,2)");
        headData.put("truncate(a2.SUMLOSSFEE,2)","核损金额");

        keywords.add("sumpaid");
        headData.put("sumpaid","总赔付金额");

        keywords.add("CHECKER1");
        headData.put("CHECKER1","查勘员");

        keywords.add("handlercode");
        headData.put("handlercode","经办人代码");

        keywords.add("handname");
        headData.put("handname","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人代码");

        keywords.add("hand1name");
        headData.put("hand1name","归属人");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("UNDERWRITEENDDATE");
        headData.put("UNDERWRITEENDDATE","核损完成时间");




        File file = new File(FileUtils.getTitle2(area+"-"+monthminonenum+"月结案清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //车险交叉
    public void  cxjiaocha(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("contractno");
        headData.put("contractno","合同号");

        keywords.add("projectcode");
        headData.put("projectcode","项目号");

        keywords.add("sumamount");
        headData.put("sumamount","总保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","总保费");

        keywords.add("jbf");
        headData.put("jbf","净保费");

        keywords.add("sumtaxpremium");
        headData.put("sumtaxpremium","税费");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("agentcode");
        headData.put("agentcode","渠道");

        keywords.add("handlercode");
        headData.put("handlercode","经办人");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");


        keywords.add("zqdfl");
        headData.put("zqdfl","主渠道费率");

        keywords.add("zqdfy");
        headData.put("zqdfy","主渠道费用");

        keywords.add("fqdfl");
        headData.put("fqdfl","副渠道费率");

        keywords.add("fqdfy");
        headData.put("fqdfy","副渠道费用");

        File file = new File(FileUtils.getTitle2("车险交叉销售清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //非车交叉
    public void  fcjiaocha(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("comcode");
        headData.put("comcode","归属机构");
        keywords.add("riskcode");
        headData.put("riskcode","险种");
        keywords.add("proposalno");
        headData.put("proposalno","投保单");
        keywords.add("policyno");
        headData.put("policyno","保单号");
        keywords.add("contractno");
        headData.put("contractno","合同号");
        keywords.add("projectcode");
        headData.put("projectcode","项目号");
        keywords.add("sumamount");
        headData.put("sumamount","总保额");
        keywords.add("sumpremium");
        headData.put("sumpremium","总保费");
        keywords.add("jbf");
        headData.put("jbf","净保费");
        keywords.add("sumtaxfee");
        headData.put("sumtaxfee","税费");
        keywords.add("startdate");
        headData.put("startdate","起保日期");
        keywords.add("enddate");
        headData.put("enddate","终保日期");
        keywords.add("operatedate");
        headData.put("operatedate","签单日期");
        keywords.add("agentcode");
        headData.put("agentcode","渠道");
        keywords.add("handlercode");
        headData.put("handlercode","经办人");
        keywords.add("handler1code");
        headData.put("handler1code","归属人");
        keywords.add("zqdfl");
        headData.put("zqdfl","主渠道费率");
        keywords.add("zqdfy");
        headData.put("zqdfy","主渠道费用");
        keywords.add("fqdfl");
        headData.put("fqdfl","副渠道费率");
        keywords.add("fqdfy");
        headData.put("fqdfy","副渠道费用");

        File file = new File(FileUtils.getTitle2("非车险交叉销售清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //车船税
    public void  chechuanshui(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("TaxPayerCode");
        headData.put("TaxPayerCode","纳税人代码");
        keywords.add("DutyPaidProofNo");
        headData.put("DutyPaidProofNo","完税凭证号/减免税证明号");
        keywords.add("TaxPrintProofNo");
        headData.put("TaxPrintProofNo","完税证明号");
        keywords.add("licenseno");
        headData.put("licenseno","车牌号");
        keywords.add("model");
        headData.put("model","车型");
        keywords.add("carkindcode");
        headData.put("carkindcode","车型种类");
        keywords.add("TaxPayerIdentNo");
        headData.put("TaxPayerIdentNo","纳税人身份证号码");
        keywords.add("ThisPayTax");
        headData.put("ThisPayTax","应缴");
        keywords.add("PrePayTax");
        headData.put("PrePayTax","补缴");
        keywords.add("DelayPayTax");
        headData.put("DelayPayTax","滞纳金");
        keywords.add("SumPayTax");
        headData.put("SumPayTax","总车船税");
        keywords.add("policyno");
        headData.put("policyno","保单号");
        keywords.add("startdate");
        headData.put("startdate","起保日期");
        keywords.add("enddate");
        headData.put("enddate","终保日期");
        keywords.add("deskdate");
        headData.put("deskdate","收付日期");
        keywords.add("comcode");
        headData.put("comcode","归属机构");
        keywords.add("TaxPayerName");
        headData.put("TaxPayerName","纳税人");
        keywords.add("TaxPayerNumber");
        headData.put("TaxPayerNumber","纳税人识别码");
        keywords.add("handlercode");
        headData.put("handlercode","经办人");
        keywords.add("TaxType");
        headData.put("TaxType","纳税类型");
        keywords.add("frameno");
        headData.put("frameno","车架码");
        keywords.add("engineno");
        headData.put("engineno","发动机码");
        keywords.add("enrolldate");
        headData.put("enrolldate","初登日期");
        keywords.add("CarLotEquQuality");
        headData.put("CarLotEquQuality","整备质量");
        keywords.add("ExhaustScale");
        headData.put("ExhaustScale","排量");
        keywords.add("kindcode");
        headData.put("kindcode","收付代码");
        keywords.add("sffcomcode");
        headData.put("sffcomcode","收付机构");
        keywords.add("mainamount");
        headData.put("mainamount","主金额");
        keywords.add("rateamount");
        headData.put("rateamount","兑换金额");
        keywords.add("agentcode");
        headData.put("agentcode","渠道");
        keywords.add("monopolycode");
        headData.put("monopolycode","推修码");
        keywords.add("monopolyname");
        headData.put("monopolyname","推修厂");
        keywords.add("ModelCode");
        headData.put("ModelCode","车型代码");
        keywords.add("BrandName");
        headData.put("BrandName","车型名称");
        keywords.add("UseNatureCode");
        headData.put("UseNatureCode","使用类别");
        keywords.add("CarKindCode");
        headData.put("CarKindCode","车型种类");
        keywords.add("TaxAbateReason");
        headData.put("TaxAbateReason","减免税原因");
        keywords.add("SeatCount");
        headData.put("SeatCount","座位数");

        File file = new File(FileUtils.getTitle2(monthminonenum+"月车船税清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //出单统计量
    public void  cdtjl(List<Map<String, Object>>datas,String title){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("车险保单");
        headData.put("车险保单","车险保单");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("ProposalNo");
        headData.put("ProposalNo","投保单号/批单号");

        keywords.add("RiskCode");
        headData.put("RiskCode","险种");


        keywords.add("inputtime");
        headData.put("inputtime","录入时间");

        keywords.add("StartDate");
        headData.put("StartDate","起保日期/批改日期");

        keywords.add("SumPremium");
        headData.put("SumPremium","总保费");

        keywords.add("operatorcode");
        headData.put("operatorcode","操作员");

        keywords.add("licenseno");
        headData.put("licenseno","车牌号码");



        File file = new File(FileUtils.getTitle2(monthminonenum+"月"+title));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //车商口径
    public void  cskj(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("sumamount");
        headData.put("sumamount","保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","保费");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("handlercode");
        headData.put("handlercode","经办人");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道码");

        keywords.add("xuzhuanbao");
        headData.put("xuzhuanbao","新续转");

        keywords.add("clauseType");
        headData.put("clauseType","条款");

        keywords.add("LicenseNo");
        headData.put("LicenseNo","车牌");

        keywords.add("FrameNo");
        headData.put("FrameNo","车架码");

        keywords.add("EngineNo");
        headData.put("EngineNo","发动机号");

        keywords.add("EnrollDate");
        headData.put("EnrollDate","初登日期");

        keywords.add("carKindCode");
        headData.put("carKindCode","车型类别");

        keywords.add("MonopolyCode");
        headData.put("MonopolyCode","推修码");

        keywords.add("MonopolyName");
        headData.put("MonopolyName","推修厂");

        keywords.add("NewCarFlag");
        headData.put("NewCarFlag","新车");


        File file = new File(FileUtils.getTitle2("车商口径清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //车险起保
    public void  cxqb(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("sumamount");
        headData.put("sumamount","保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","保费");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("handlercode");
        headData.put("handlercode","经办人");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道码");

        keywords.add("clauseType");
        headData.put("clauseType","条款");

        keywords.add("LicenseNo");
        headData.put("LicenseNo","车牌");

        keywords.add("FrameNo");
        headData.put("FrameNo","车架码");

        File file = new File(FileUtils.getTitle2("车险起保清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }
    //车险签单
    public void cxqd(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("sumamount");
        headData.put("sumamount","保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","保费");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("handlercode");
        headData.put("handlercode","经办人");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道码");

        keywords.add("clauseType");
        headData.put("clauseType","条款");

        keywords.add("LicenseNo");
        headData.put("LicenseNo","车牌");

        keywords.add("FrameNo");
        headData.put("FrameNo","车架码");


        File file = new File(FileUtils.getTitle2("车险签单清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }
    //非车起保
    public void  fcqb(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("sumamount");
        headData.put("sumamount","保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","保费");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("handlercode");
        headData.put("handlercode","经办人");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道码");

        File file = new File(FileUtils.getTitle2("非车起保清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //非车签单
    public void  fcqd(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("proposalno");
        headData.put("proposalno","投保单");

        keywords.add("policyno");
        headData.put("policyno","保单");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("startdate");
        headData.put("startdate","编号");

        keywords.add("enddate");
        headData.put("enddate","起保日期");

        keywords.add("sumamount");
        headData.put("sumamount","终保日期");

        keywords.add("sumpremium");
        headData.put("sumpremium","保额");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","保费");

        keywords.add("handlercode");
        headData.put("handlercode","净保费");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("handler1code");
        headData.put("handler1code","归属人");

        keywords.add("agentcode");
        headData.put("agentcode","渠道码");


        File file = new File(FileUtils.getTitle2("非车签单清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //邮政到期
    public void  yzdq(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();
        keywords.add("proposalno");
        headData.put("proposalno","投保单号");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("ComCode");
        headData.put("ComCode","归属机构");

        keywords.add("insuredname");
        headData.put("insuredname","投保人");

        keywords.add("identifynumber");
        headData.put("identifynumber","投保人身份证");

        keywords.add("bbr");
        headData.put("bbr","被保人");

        keywords.add("bbrsfz");
        headData.put("bbrsfz","被保人身份证");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("sumAmount");
        headData.put("sumAmount","保额");

        keywords.add("sumpremium");
        headData.put("sumpremium","保费");

        keywords.add("modename");
        headData.put("modename","方案代码名称");

        keywords.add("phonenumber");
        headData.put("phonenumber","联系电话");

        keywords.add("mobile");
        headData.put("mobile","手机");


        File file = new File(FileUtils.getTitle2( nextmonth+"-"+nexttowmonth+"邮政到期清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }

    //保贝商城
    public void bbsc (List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("comname");
        headData.put("comname","支公司名称");

        keywords.add("comcodename");
        headData.put("comcodename","网点名称");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("sumpremium");
        headData.put("sumpremium","应交保费");

        keywords.add("sumtaxpremium");
        headData.put("sumtaxpremium","增值税");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","纯保费");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("clausetype");
        headData.put("clausetype","条款产品");

        keywords.add("carkindname");
        headData.put("carkindname","车辆种类名称");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("licenseno");
        headData.put("licenseno","号牌号码");

        keywords.add("BusinessNatureName");
        headData.put("BusinessNatureName","业务来源");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("riskcode");
        headData.put("riskcode","险种代码");

        keywords.add("MonopolyCode");
        headData.put("MonopolyCode","送修码");

        keywords.add("MonopolyName");
        headData.put("MonopolyName","送修码名称");

        keywords.add("handler1name");
        headData.put("handler1name","归属人员名称");

        keywords.add("handlername");
        headData.put("handlername","经办人员名称");

        keywords.add("policynotime");
        headData.put("policynotime","转保单日期");

        keywords.add("ProjectCode");
        headData.put("ProjectCode","项目代码");

        keywords.add("insuredname");
        headData.put("insuredname","投保人名称");

        keywords.add("bbrinsuredname");
        headData.put("bbrinsuredname","被保险人名称");

        keywords.add("xinxuzhuan");
        headData.put("xinxuzhuan","新续传标志");

        keywords.add("mainsellfeerate");
        headData.put("mainsellfeerate","主渠道跟单比例");

        keywords.add("sellfeerate");
        headData.put("sellfeerate","副渠道跟单比例");

        keywords.add("summainsellfeerate");
        headData.put("summainsellfeerate","主渠道总比例");

        keywords.add("sumsellfeerate");
        headData.put("sumsellfeerate","副渠道总比例");

        keywords.add("maxcostrate");
        headData.put("maxcostrate","费用上限");

        keywords.add("lastpolicyno");
        headData.put("lastpolicyno","上年保单");

        keywords.add("useNatureName");
        headData.put("useNatureName","使用性质");

        keywords.add("insuredtype");
        headData.put("insuredtype","客户类型");

        keywords.add("clausecode");
        headData.put("clausecode","特约代码");

        keywords.add("clausename");
        headData.put("clausename","特约名称");

        keywords.add("clauses");
        headData.put("clauses","特约内容");

        File file = new File(FileUtils.getTitle("每日保贝商城清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);

    }
    public void bbsc2(List<Map<String, Object>>datas,List<Map<String, Object>> total){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("clausecode");
        headData.put("clausecode","特约码");

        keywords.add("clausename");
        headData.put("clausename","特约码内容");

        keywords.add("sumjifen");
        headData.put("sumjifen","积分");

        keywords.add("count");
        headData.put("count","数量");



        File file = new File(FileUtils.getTitle("每日保贝商城积分汇总清单"));
        FileUtils.saveExcelFile(keywords, headData, "当天积分汇总：" + total,file,datas);

    }
    //保呗商城月
    public void bbscmonth (List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("comname");
        headData.put("comname","支公司名称");

        keywords.add("comcodename");
        headData.put("comcodename","网点名称");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("sumpremium");
        headData.put("sumpremium","应交保费");

        keywords.add("sumtaxpremium");
        headData.put("sumtaxpremium","增值税");

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","纯保费");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("clausetype");
        headData.put("clausetype","条款产品");

        keywords.add("carkindname");
        headData.put("carkindname","车辆种类名称");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("licenseno");
        headData.put("licenseno","号牌号码");

        keywords.add("BusinessNatureName");
        headData.put("BusinessNatureName","业务来源");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("riskcode");
        headData.put("riskcode","险种代码");

        keywords.add("MonopolyCode");
        headData.put("MonopolyCode","送修码");

        keywords.add("MonopolyName");
        headData.put("MonopolyName","送修码名称");

        keywords.add("handler1name");
        headData.put("handler1name","归属人员名称");

        keywords.add("handlername");
        headData.put("handlername","经办人员名称");

        keywords.add("policynotime");
        headData.put("policynotime","转保单日期");

        keywords.add("ProjectCode");
        headData.put("ProjectCode","项目代码");

        keywords.add("insuredname");
        headData.put("insuredname","投保人名称");

        keywords.add("bbrinsuredname");
        headData.put("bbrinsuredname","被保险人名称");

        keywords.add("xinxuzhuan");
        headData.put("xinxuzhuan","新续传标志");

        keywords.add("mainsellfeerate");
        headData.put("mainsellfeerate","主渠道跟单比例");

        keywords.add("sellfeerate");
        headData.put("sellfeerate","副渠道跟单比例");

        keywords.add("summainsellfeerate");
        headData.put("summainsellfeerate","主渠道总比例");

        keywords.add("sumsellfeerate");
        headData.put("sumsellfeerate","副渠道总比例");

        keywords.add("maxcostrate");
        headData.put("maxcostrate","费用上限");

        keywords.add("lastpolicyno");
        headData.put("lastpolicyno","上年保单");

        keywords.add("useNatureName");
        headData.put("useNatureName","使用性质");

        keywords.add("insuredtype");
        headData.put("insuredtype","客户类型");

        keywords.add("clausecode");
        headData.put("clausecode","特约代码");

        keywords.add("clausename");
        headData.put("clausename","特约名称");

        keywords.add("clauses");
        headData.put("clauses","特约内容");

        File file = new File(FileUtils.getTitle2(monthminonenum+"月保贝商城清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + monthminonenum+"月",file,datas);

    }
    public void bbsc2month(List<Map<String, Object>>datas,List<Map<String, Object>> total){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();

        keywords.add("sumnetpremium");
        headData.put("sumnetpremium","净保费");

        keywords.add("clausecode");
        headData.put("clausecode","特约码");

        keywords.add("clausename");
        headData.put("clausename","特约码内容");

        keywords.add("sumjifen");
        headData.put("sumjifen","积分");

        keywords.add("count");
        headData.put("count","数量");



        File file = new File(FileUtils.getTitle2(monthminonenum+"月保贝商城积分汇总清单"));
        FileUtils.saveExcelFile(keywords, headData, "当月积分汇总：" + total,file,datas);

    }
    //异地车
    public void ydc(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();


        keywords.add("comname");
        headData.put("comname","支公司名称");

        keywords.add("comcodename");
        headData.put("comcodename","网点名称");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("sumpremium");
        headData.put("sumpremium","应交保费");

        keywords.add("sumtaxpremium");
        headData.put("sumtaxpremium","增值税");

        keywords.add("agentcode");
        headData.put("agentcode","渠道代码");

        keywords.add("agentname");
        headData.put("agentname","渠道名称");

        keywords.add("clausetype");
        headData.put("clausetype","条款产品");

        keywords.add("carkindcode");
        headData.put("carkindcode","车辆种类名称");

        keywords.add("startdate");
        headData.put("startdate","起保日期");

        keywords.add("enddate");
        headData.put("enddate","终保日期");

        keywords.add("UnderWriteFlag");
        headData.put("UnderWriteFlag","核保标志");


        keywords.add("licenseno");
        headData.put("licenseno","号牌号码");

        keywords.add("operatedate");
        headData.put("operatedate","签单日期");

        keywords.add("riskcode");
        headData.put("riskcode","险种代码");

        keywords.add("MonopolyCode");
        headData.put("MonopolyCode","送修码");

        keywords.add("MonopolyName");
        headData.put("MonopolyName","送修码名称");

        keywords.add("policynotime");
        headData.put("policynotime","转保单日期");

        keywords.add("toubaorenName");
        headData.put("toubaorenName","投保人名称");

        keywords.add("InsuredName");
        headData.put("InsuredName","被保险人名称");

        keywords.add("IdentifyNumber");
        headData.put("IdentifyNumber","身份证");

        keywords.add("InsuredAddress");
        headData.put("InsuredAddress","住址");

        keywords.add("xinxuzhuan");
        headData.put("xinxuzhuan","新续传标志");

        keywords.add("useNatureName");
        headData.put("useNatureName","使用性质");

        keywords.add("province");
        headData.put("province","所属省份");


        File file = new File(FileUtils.getTitle2( monthminonenum+"月异地车清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }
    //批改
    public void pigai(List<Map<String, Object>>datas){
        ArrayList<String> keywords = new ArrayList<>();
        Map<String, String> headData = new HashMap<>();


        keywords.add("licenseno");
        headData.put("licenseno","批改后");

        keywords.add("comcode");
        headData.put("comcode","归属机构");

        keywords.add("riskcode");
        headData.put("riskcode","险种");

        keywords.add("endorseno");
        headData.put("endorseno","批单号");

        keywords.add("policyno");
        headData.put("policyno","保单号");

        keywords.add("proposalno");
        headData.put("proposalno","投保单号");

        keywords.add("licenseno1");
        headData.put("licenseno1","批改前");

        keywords.add("clausetype");
        headData.put("clausetype","条款");

        keywords.add("carkindcode");
        headData.put("carkindcode","车型");

        keywords.add("endordate");
        headData.put("endordate","批改时间");

        keywords.add("endortype");
        headData.put("endortype","批改类型");

        keywords.add("carkindcode");
        headData.put("carkindcode","是否新车");


        File file = new File(FileUtils.getTitle2( lastweek+"-"+todayminone+"批改清单"));
        FileUtils.saveExcelFile(keywords, headData, "日期: " + date,file,datas);
    }


//    public void exportToExcelT(List<Map<String, Object>>datas,float sum) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("id");
//        headData.put("id","编号");
//
//        keywords.add("policyno");
//        headData.put("policyno","保单号");
//
//        keywords.add("sumpremium");
//        headData.put("sumpremium","保费");
//
//        keywords.add("customerservice");
//        headData.put("customerservice","特约代码");
//
//        keywords.add("points");
//        headData.put("points","积分数量");
//
//        keywords.add("reason");
//        headData.put("reason","充值条款");
//
//        keywords.add("username");
//        headData.put("username","姓名");
//
//        keywords.add("identifynumber");
//        headData.put("identifynumber","身份证号");
//
//        keywords.add("mobile");
//        headData.put("mobile","手机号码");
//
//        keywords.add("bankcard");
//        headData.put("bankcard","银行卡号");
//
//
//        keywords.add("carid");
//        headData.put("carid","商盈通卡号");
//
//        keywords.add("licenseno");
//        headData.put("licenseno","车牌号");
//
//        keywords.add("inserttime");
//        headData.put("inserttime","插入时间");
//
//        String sum1 = String.valueOf(sum);
//
//        File file = new File(FileUtils.getTitle("每日充值成功清单"));
//        FileUtils.saveExcelFile(keywords, headData, "充值日期: " + DateTimeUtils.getdatetime()+"--------------充值总额："+sum1, file,datas);
//
//    }
//    public void exportToExcelF(List<Map<String, Object>> datas) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("id");
//        headData.put("id","编号");
//
//        keywords.add("policyno");
//        headData.put("policyno","保单号");
//
//        keywords.add("sumpremium");
//        headData.put("sumpremium","保费");
//
//        keywords.add("customerservice");
//        headData.put("customerservice","特约代码");
//
//        keywords.add("points");
//        headData.put("points","积分数量");
//
//        keywords.add("reason");
//        headData.put("reason","充值条款");
//
//        keywords.add("username");
//        headData.put("username","姓名");
//
//        keywords.add("identifynumber");
//        headData.put("identifynumber","身份证号");
//
//        keywords.add("mobile");
//        headData.put("mobile","手机号码");
//
//        keywords.add("bankcard");
//        headData.put("bankcard","银行卡号");
//
//
//        keywords.add("carid");
//        headData.put("carid","商盈通卡号");
//
//        keywords.add("licenseno");
//        headData.put("licenseno","车牌号");
//
//        keywords.add("inserttime");
//        headData.put("inserttime","插入时间");
//
//
//
//        File file = new File(FileUtils.getTitle("每日充值失败清单"));
//        FileUtils.saveExcelFile(keywords, headData, "充值日期: " + DateTimeUtils.getdatetime(), file,datas);
//
//    }
//    public void exportToExcelP(List<Map<String, Object>> datas) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("id");
//        headData.put("id","编号");
//
//        keywords.add("policyno");
//        headData.put("policyno","保单号");
//
//        keywords.add("sumpremium");
//        headData.put("sumpremium","保费");
//
//        keywords.add("customerservice");
//        headData.put("customerservice","特约代码");
//
//        keywords.add("points");
//        headData.put("points","积分数量");
//
//        keywords.add("reason");
//        headData.put("reason","充值条款");
//
//        keywords.add("username");
//        headData.put("username","姓名");
//
//        keywords.add("identifynumber");
//        headData.put("identifynumber","身份证号");
//
//        keywords.add("mobile");
//        headData.put("mobile","手机号码");
//
//        keywords.add("bankcard");
//        headData.put("bankcard","银行卡号");
//
//
//        keywords.add("carid");
//        headData.put("carid","商盈通卡号");
//
//        keywords.add("licenseno");
//        headData.put("licenseno","车牌号");
//
//        keywords.add("inserttime");
//        headData.put("inserttime","插入时间");
//
//
////        File file = new File(FileUtils.getTitle2("出单清单"));
////        FileUtils.saveExcelFile(keywords, headData, "获取日期: " + DateTimeUtils.getdatetime(), file,datas);
//        File file = new File(FileUtils.getTitle("批改清单"));
//        FileUtils.saveExcelFile(keywords, headData, "批改日期: " + DateTimeUtils.getdatetime(), file,datas);
//
//    }
//
//    public void exportToExcelBdmessage(List<Map<String, Object>> datas) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("customerservice");
//        headData.put("customerservice","条款码");
//
//        keywords.add("proposalno");
//        headData.put("proposalno","投保单号");
//
//        keywords.add("policyno");
//        headData.put("policyno","保单号");
//
//        keywords.add("insuredname");
//        headData.put("insuredname","被保险人");
//
//        keywords.add("identifynumber");
//        headData.put("identifynumber","身份证号");
//
//        keywords.add("sumpremium");
//        headData.put("sumpremium","商业险保费");
//
//        keywords.add("chunbaofei");
//        headData.put("chunbaofei","纯保费");
//
//        keywords.add("points");
//        headData.put("points","积分");
//
//        keywords.add("zhuqudaorate");
//        headData.put("zhuqudaorate","主渠道跟单比例");
//
//        keywords.add("fuqudaorate");
//        headData.put("fuqudaorate","副渠道跟单比例");
//
//        keywords.add("operatedate");
//        headData.put("operatedate","最后操作日期");
//
//        keywords.add("startdate");
//        headData.put("startdate","起保日期");
//
//        keywords.add("enddate");
//        headData.put("enddate","终保日期");
//
//        keywords.add("qudao");
//        headData.put("qudao","渠道");
//
//        keywords.add("clausetype");
//        headData.put("clausetype","类型");
//
//        keywords.add("flag");
//        headData.put("flag","新续转");
//
//        keywords.add("licenseno");
//        headData.put("licenseno","车牌号");
//
//        keywords.add("vinno");
//        headData.put("vinno","vin码");
//
//        keywords.add("engineno");
//        headData.put("engineno","发动机码");
//
//        keywords.add("frameno");
//        headData.put("frameno","车架码");
//
//        keywords.add("monopolycode");
//        headData.put("monopolycode","推荐送修码");
//
//        keywords.add("handler1code");
//        headData.put("handler1code","归属人");
//
//        keywords.add("handlercode");
//        headData.put("handlercode","经办人");
//
//
//        File file = new File(FileUtils.getTitle2("每日出单清单"));
//        FileUtils.saveExcelFile(keywords, headData, "获取日期: " + DateTimeUtils.getdatetime(), file,datas);
//    }
//    public void exportToExcelDailyPool(List<Map<String, Object>> datas) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("ttotalpoints");
//        headData.put("ttotalpoints","测试993770*0.5");
//
//        keywords.add("atotalpoints");
//        headData.put("atotalpoints","9908983*0.05");
//
//        keywords.add("btotalpoints");
//        headData.put("btotalpoints","9908985*0.10");
//
//        keywords.add("ctotalpoints");
//        headData.put("ctotalpoints","9908987*0.15");
//
//        keywords.add("dtotalpoints");
//        headData.put("dtotalpoints","9908988*0.20");
//
//        keywords.add("etotalpoints");
//        headData.put("etotalpoints","9908989*0.25");
//
//        keywords.add("ftotalpoints");
//        headData.put("ftotalpoints","9908991*0.30");
//
//        keywords.add("gtotalpoints");
//        headData.put("gtotalpoints","9908992*0.35");
//
//        keywords.add("htotalpoints");
//        headData.put("htotalpoints","9908993*0.40");
//
//        keywords.add("itotalpoints");
//        headData.put("itotalpoints","9908994*0.45");
//
//        keywords.add("DailyTotalpoints");
//        headData.put("DailyTotalpoints","当天应充值积分");
//
//        keywords.add("accumulativetotal0");
//        headData.put("accumulativetotal0","累计应充值");
//
//        keywords.add("accumulativetotal1");
//        headData.put("accumulativetotal1","累计已充值");
//
//        File file = new File(FileUtils.getTitle2("每日积分汇总清单"));
//        FileUtils.saveExcelFile(keywords, headData, "获取日期: " + DateTimeUtils.getdatetime(), file,datas);
//    }
//    public void exportToExcelczsum(List<Map<String, Object>> datas,float sum) {
//        ArrayList<String> keywords = new ArrayList<>();
//        Map<String, String> headData = new HashMap<>();
//
//        keywords.add("id");
//        headData.put("id","编号");
//
//        keywords.add("policyno");
//        headData.put("policyno","保单号");
//
//        keywords.add("sumpremium");
//        headData.put("sumpremium","保费");
//
//        keywords.add("customerservice");
//        headData.put("customerservice","特约代码");
//
//        keywords.add("points");
//        headData.put("points","积分数量");
//
//        keywords.add("reason");
//        headData.put("reason","充值条款");
//
//        keywords.add("username");
//        headData.put("username","姓名");
//
//        keywords.add("identifynumber");
//        headData.put("identifynumber","身份证号");
//
//        keywords.add("mobile");
//        headData.put("mobile","手机号码");
//
//        keywords.add("bankcard");
//        headData.put("bankcard","银行卡号");
//
//
//        keywords.add("carid");
//        headData.put("carid","商盈通卡号");
//
//        keywords.add("licenseno");
//        headData.put("licenseno","车牌号");
//
//        keywords.add("inserttime");
//        headData.put("inserttime","插入时间");
//
//        String sum1 = String.valueOf(sum);
//
//        File file = new File(FileUtils.getTitle("累计充值成功清单"));
//        FileUtils.saveExcelFile(keywords, headData, "充值日期: " + DateTimeUtils.getdatetime()+"----------------累计充值:"+ sum1, file,datas);
//
//    }
}
