package com.example.etl.services;

import com.example.etl.domain.ResultMap3;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.informix.jdbc.IfxTmpFile.e;

@Service
public class Xiaoguan {
    @Autowired
    @Qualifier("picczq")
    protected JdbcTemplate picczq;
    @Autowired
    private ExportToExcel exportToExcel;

    public ResultMap3 cheshang(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("Create temporary table temp1 (\n" +
                "select proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode,cast(null as char(20)) xuzhuanbao\n" +
                "from cx_prpcmain\n" +
                "where startdate >= ? and startdate <= ?\n" +
                "and comcode in ('44129304','44129306','44129305','44129320','44129331','44129332','44129335','44129336','44129342','44129346')\n" +
                "and policyno != ''\n" +
                "and policyno is not null);", startdate, enddate);
        picczq.update("create index proposalno on temp1(proposalno);");
        picczq.update("create index policyno on temp1(policyno);");
        picczq.update("create temporary table temp2(\n" +
                "select proposalno,clauseType,LicenseNo,FrameNo,EngineNo,EnrollDate,carKindCode,\n" +
                "MonopolyCode,MonopolyName,NewCarFlag\n" +
                "from cx_prpcitem_car \n" +
                "where proposalno in (select proposalno from temp1));");
        picczq.update("create index temp2_proposalno on temp2(proposalno);");
        picczq.update("create temporary table temp3(\n" +
                "select proposalno,policyno,oldpolicyno\n" +
                "from cx_prpcrenewal \n" +
                "where proposalno in (select proposalno from temp1));");
        picczq.update("update temp1 set xuzhuanbao = '转保';");
        picczq.update("update temp1,temp2 set temp1.xuzhuanbao = '新保' \n" +
                "where temp1.proposalno = temp2.proposalno\n" +
                "and temp2.NewCarFlag = 1;");
        picczq.update("update temp1,temp3 set temp1.xuzhuanbao = '续保' \n" +
                "where temp1.proposalno = temp3.proposalno;");

        picczq.update("create temporary table temp4 (\n" +
                "select a.proposalno,a.policyno,a.riskcode,a.operatedate,a.startdate,\n" +
                "a.enddate,a.sumamount,a.sumpremium,a.sumnetpremium,a.handlercode,a.comcode,a.handler1code,\n" +
                "a.agentcode,a.xuzhuanbao,b.clausetype,b.licenseno,b.frameno,b.engineno,b.enrolldate,\n" +
                "b.carkindcode,b.monopolycode,b.monopolyname,b.newcarflag\n" +
                "from temp1 a,temp2 b\n" +
                "where b.proposalno = a.proposalno order by startdate,comcode);");

        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp4 order by startdate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp4 order by startdate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp4 order by startdate desc;");


        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp1,temp2,temp3,temp4;");
        exportToExcel.cskj(dataall);

        return new ResultMap3(1, "", true, "", datepage, datenum2);

    }

    public ResultMap3 chexianqibao(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("create temporary table temp1 (\n" +
                "select proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode,cast('转保' as char(20))xinxuzhuan\n" +
                "from cx_prpcmain\n" +
                "where (startdate >=? and startdate<=? and policyno like 'p%'));", startdate, enddate);
        picczq.update("create index proposalno on temp1(proposalno);");
        picczq.update("create index policyno on temp1(policyno);");
        picczq.update("create temporary table temp2(\n" +
                "select proposalno,clausetype,licenseno,frameno,newcarflag\n" +
                "from cx_prpcitem_car \n" +
                "where proposalno in (select proposalno from temp1));");
        picczq.update("create index temp2_proposalno on temp2(proposalno);");
        picczq.update("create temporary table temp22(\n" +
                "select proposalno,policyno,oldpolicyno\n" +
                "from cx_prpcrenewal \n" +
                "where proposalno in (select proposalno from temp1));");
        picczq.update("update temp1,temp2 set xinxuzhuan = '新保'\n" +
                "where temp1.proposalno = temp2.proposalno\n" +
                "and temp2.newcarflag = 1;");
        picczq.update("update temp1,temp22 set temp1.xinxuzhuan = '续保' \n" +
                "where temp1.proposalno = temp22.proposalno;");
        picczq.update("create temporary table temp3(\n" +
                "select a.proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode,\n" +
                "clausetype,licenseno,frameno,a.xinxuzhuan from temp1 a,temp2 b \n" +
                "where b.proposalno = a.proposalno order by startdate);");
        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp3 order by startdate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp3 order by startdate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp3 order by startdate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp1,temp2,temp22,temp3;");
        exportToExcel.cxqb(dataall);


        return new ResultMap3(1, "", true, "", datepage, datenum2);

    }

    public ResultMap3 chexianjiaochaxiaoshou(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("CREATE TEMPORARY TABLE cx1\n" +
                "select a.comcode,a.riskcode,a.proposalno,a.policyno,a.contractno,a.projectcode,a.sumamount,a.sumpremium,a.sumpremium-a.sumtaxpremium as jbf,a.sumtaxpremium,\n" +
                "a.startdate,a.enddate,a.operatedate,a.agentcode,a.handlercode,a.handler1code,\n" +
                "cast('' as NCHAR(100)) as zqdfl,\n" +
                "cast('' as NCHAR(100)) as zqdfy,\n" +
                "cast('' as NCHAR(100)) as fqdfl,\n" +
                "cast('' as NCHAR(100)) as fqdfy\n" +
                "from cx_prpcmain a\t\n" +
                "where a.underwriteflag in ('1','3')\n" +
                "and a.agentcode  in ('44003F100013','44013J200001','44003J200022')\n" +
                "and a.operatedate  >=? and  a.operatedate <= ?;", startdate, enddate);
        picczq.update("CREATE  INDEX policyno ON cx1 (policyno);");
        picczq.update("update cx1 set zqdfl = (select sum(sellfeerate) from cx_prpcseller \n" +
                "where cx1.policyno = cx_prpcseller.policyno \n" +
                "and cx_prpcseller.mainflag = '1');");
        picczq.update("update cx1 set zqdfy = (select sum(sellfee) from cx_prpcseller \n" +
                "where cx1.policyno = cx_prpcseller.policyno \n" +
                "and cx_prpcseller.mainflag = '1');");
        picczq.update("update cx1 set fqdfl = (select sum(sellfeerate) from cx_prpcseller \n" +
                "where cx1.policyno = cx_prpcseller.policyno \n" +
                "and cx_prpcseller.mainflag = '0');");
        picczq.update("update cx1 set fqdfy = (select sum(sellfee) from cx_prpcseller \n" +
                "where cx1.policyno = cx_prpcseller.policyno \n" +
                "and cx_prpcseller.mainflag = '0');");
        List<Map<String, Object>> dataall = picczq.queryForList("select * from cx1 order by operatedate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from cx1 order by operatedate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from cx1 order by operatedate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table cx1;");
        exportToExcel.cxjiaocha(dataall);


        return new ResultMap3(1, "", true, "", datepage, datenum2);
    }

    public ResultMap3 chexianqiandan(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("Create TEMPORARY table temp1 (\n" +
                "select proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode\n" +
                "from cx_prpcmain\n" +
                "where operatedate>= '2018-04-01' and operatedate<'2018-05-01'\n" +
                "and agentcode in ('44003F100013','44013J200001','44003J200022')\n" +
                "and policyno != ''\n" +
                "and policyno is not null);");
        picczq.update("create index proposalno on temp1(proposalno);");
        picczq.update("create index policyno on temp1(policyno);");
        picczq.update("create temporary table temp2(\n" +
                "select proposalno,clauseType,LicenseNo,FrameNo\n" +
                "from cx_prpcitem_car \n" +
                "where proposalno in (select proposalno from temp1));");
        picczq.update("create index temp2_proposalno on temp2(proposalno);");
        picczq.update("Create TEMPORARY table temp3 (\n" +
                "select a.proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode,\n" +
                "clauseType,LicenseNo,FrameNo from temp1 a,temp2 b\n" +
                "where b.proposalno = a.proposalno);");
        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp3 order by operatedate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp3 order by operatedate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp3 order by operatedate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp1,temp2,temp3;");
        exportToExcel.cxqd(dataall);

        return new ResultMap3(1, "", true, "", datepage, datenum2);
    }

    public ResultMap3 feicheqibao(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("create temporary table temp1 (\n" +
                "select proposalno,policyno,riskcode,operatedate,startdate,\n" +
                "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode\n" +
                "from fc_prpcmain\n" +
                "where startdate>=?\n" +
                "and startdate<=?\n" +
                "and policyno like '%p%'\n" +
                "and riskcode not in ('hu3','mse','msd','muw','iay','ian','iau','hu2','iln','iji','ig6','mpl','iul'));", startdate, enddate);
        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp1 order by startdate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp1 order by startdate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp1 order by startdate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp1;");
        exportToExcel.fcqb(dataall);


        return new ResultMap3(1, "", true, "", datepage, datenum2);
    }
    public ResultMap3 feicheqiandan(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
       picczq.update("Create TEMPORARY table temp1 (\n" +
               "select proposalno,policyno,riskcode,operatedate,startdate,\n" +
               "enddate,sumamount,sumpremium,sumnetpremium,handlercode,comcode,handler1code,agentcode\n" +
               "from fc_prpcmain\n" +
               "where operatedate>=?\n" +
               "and operatedate<=? \n" +
               "and agentcode in ('44003F100013','44013J200001','44003J200022')\n" +
               "and policyno != ''\n" +
               "and policyno is not null);",startdate,enddate);
        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp1 order by operatedate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp1 order by operatedate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp1 order by operatedate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp1;");
        exportToExcel.fcqd(dataall);


        return new ResultMap3(1, "", true, "", datepage, datenum2);
    }
    public ResultMap3 feichejiaochaxiaoshou(String page, String startdate, String enddate) {

        int num = Integer.parseInt(page);
        int num3 = (num - 1) * 50;
        picczq.update("CREATE TEMPORARY TABLE fc1\n" +
                "select a.comcode,a.riskcode,a.proposalno,a.policyno,a.contractno,a.projectcode,a.sumamount,a.sumpremium,a.sumpremium-a.sumtaxfee as jbf,a.sumtaxfee,\n" +
                "a.startdate,a.enddate,a.operatedate,a.agentcode,a.handlercode,a.handler1code,\n" +
                "cast('' as NCHAR(100)) as zqdfl,\n" +
                "cast('' as NCHAR(100)) as zqdfy,\n" +
                "cast('' as NCHAR(100)) as fqdfl,\n" +
                "cast('' as NCHAR(100)) as fqdfy\n" +
                "from fc_prpcmain a\t\n" +
                "where a.underwriteflag in ('1','3')\n" +
                "and a.agentcode  in ('44003F100013','44013J200001','44003J200022')\n" +
                "and a.operatedate >= ?  and a.operatedate <= ?;",startdate,enddate);
        picczq.update("CREATE  INDEX policyno ON fc1 (policyno);");
        picczq.update("update fc1 set zqdfl = (select sum(sellfeerate) from fc_prpcseller \n" +
                "where fc1.policyno = fc_prpcseller.policyno \n" +
                "and fc_prpcseller.mainflag = '1');");
        picczq.update("update fc1 set zqdfy = (select sum(sellfee) from fc_prpcseller \n" +
                "where fc1.policyno = fc_prpcseller.policyno \n" +
                "and fc_prpcseller.mainflag = '1');");
        picczq.update("update fc1 set fqdfl = (select sum(sellfeerate) from fc_prpcseller \n" +
                "where fc1.policyno = fc_prpcseller.policyno \n" +
                "and fc_prpcseller.mainflag = '0');");
        picczq.update("update fc1 set fqdfy = (select sum(sellfee) from fc_prpcseller \n" +
                "where fc1.policyno = fc_prpcseller.policyno \n" +
                "and fc_prpcseller.mainflag = '0');");


        List<Map<String, Object>> dataall = picczq.queryForList("select * from fc1 order by operatedate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from fc1 order by operatedate desc limit ?,50;", num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from fc1 order by operatedate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table fc1;");
            exportToExcel.fcjiaocha(dataall);


        return new ResultMap3(1, "", true, "", datepage, datenum2);
    }
}

 