package com.example.etl.services;

import com.example.etl.domain.ResultMap;
import com.example.etl.domain.ResultMap3;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class Lipei {
    @Autowired
    @Qualifier("picczq")
    protected JdbcTemplate picczq;
    @Autowired
    private ExportToExcel exportToExcel;

    public ResultMap3 baoan(String page, String startdate, String enddate){
        int num = Integer.parseInt(page);
        int num3 = (num-1)*50;

        picczq.update("CREATE TEMPORARY TABLE baoan\n" +
                "select  a.makecom,c.licenseno,c.policyno,a.registno,a.reportdate,a.reportorname,a.reportornumber,a.damagedate,a.damagehour,a.damageaddress,a.damagename,\n" +
                "b.lflag,b.checker1,b.sumestimatefee,b. firstsiteflag\n" +
                "from cl_prplregist a,\n" +
                "cl_prplregistsummary c,\n" +
                "cl_prplchecktask b\n" +
                "where   a.validflag='1' \n" +
                "and a.reportdate >= ?\n" +
                "and a.reportdate <= ?\n" +
                "and a.registno=c.registno \n" +
                "and a.registno=b.registno;",startdate,enddate);
        picczq.update("create index policyno on baoan(policyno);");
        picczq.update("CREATE TEMPORARY TABLE baoan1\n" +
                "select a.*,b.handlercode,cast('' as NCHAR(100)) as handlername,b.handler1code,\n" +
                "cast('' as NCHAR(100)) as handler1name\n" +
                "from baoan a,\n" +
                "cx_prpcmain b\n" +
                "where a.policyno=b.policyno;");
        picczq.update("update baoan1 set handlername = (select max(username) from utiihandler a \n" +
                "where baoan1.handlercode = a.usercode);");
        picczq.update("update baoan1 set handler1name = (select max(username) from utiihandler a \n" +
                "where baoan1.handler1code = a.usercode);");
        List<Map<String, Object>> dataall = picczq.queryForList("select * from baoan1 order by reportdate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from baoan1 order by reportdate desc limit ?,50;",num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from baoan1 order by reportdate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table baoan,baoan1;");
        exportToExcel.baoan1(dataall);
        return new ResultMap3(1, "", true, "", datepage,datenum2);
    }
    public ResultMap3 jieban(String page, String startdate, String enddate){
        int num = Integer.parseInt(page);
        int num3 = (num-1)*50;
        picczq.update("create temporary table jiean\n" +
                "select a2.comcode,a0.registno,b0.claimno,a2.lflag,b1.licenseno,b1.brandname,b0.policyno,b0.insuredname,b0.agentcode,b0.startdate,\n" +
                "b0.enddate,b0.businessnature,a0.damagedate,a0.damageaddress,a0.reportdate,c.endcasedate,a0.reportornumber,a2.repairfactorycode,a2.repairfactoryname,\n" +
                "a2.handlername,a2.deflossdate,a2.underwritename,a2.sumverilossfee,a2.sumlossfee,c.sumpaid,\n" +
                "cast('' as nchar(100)) as checker1,b.handlercode,\n" +
                "cast('' as nchar(100)) as handname,b.handler1code,\n" +
                "cast('' as nchar(100)) as hand1name,\n" +
                "cast('' as nchar(100)) as agentname,a2.underwriteenddate\n" +
                "from  cl_prplregist a0,cl_prpldeflossmain a2,cl_prplcmain b0,cl_prplcitemcar b1,cx_prpcmain b,cl_prplclaim c\n" +
                "where substring(a2.underwriteflag,1,1) in ('1','3') \n" +
                "and a2.validflag='1' \n" +
                "and c.endcasedate >= ?\n" +
                "and c.endcasedate <= ?\n" +
                "and c.casetype='2'\n" +
                "and c.endcasedate is not null\n" +
                "and a2.registno=b0.registno \n" +
                "and a2.riskcode=b0.riskcode \n" +
                "and b0.id=b1.prplcmainid \n" +
                "and a0.registno=c.registno\n" +
                "and a2.registno=a0.registno\n" +
                "and b0.policyno=b.policyno;",startdate,enddate);
        picczq.update("update jiean set handname = (select max(username) from utiihandler \n" +
                "where jiean.handlercode = utiihandler.usercode);");
        picczq.update("update jiean set checker1 = (select max(checker1) from cl_prplchecktask \n" +
                "where jiean.registno = cl_prplchecktask.registno);");
        picczq.update("update jiean set hand1name = (select max(username) from utiihandler \n" +
                "where jiean.handler1code = usercode);");
        picczq.update("update jiean set agentname = (select agentname from prpdagent \n" +
                "where jiean.agentcode = prpdagent.agentcode);");
        List<Map<String, Object>> dataall = picczq.queryForList("select * from jiean order by endcasedate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from jiean order by endcasedate desc limit ?,50;",num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from jiean order by endcasedate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table jiean;");
        exportToExcel.jiean(dataall);
        return new ResultMap3(1, "", true, "", datepage,datenum2);
    }
    public ResultMap3 songxiu(String page, String startdate, String enddate){
        int num = Integer.parseInt(page);
        int num3 = (num-1)*50;
        picczq.update("create temporary table songxiu\n" +
                "select a2.comcode,a0.registno,b0.claimno,a2.lflag,b1.licenseno,b1.brandname,b0.policyno,b0.insuredname,b0.agentcode,b0.startdate,\n" +
                "b0.enddate,b0.businessnature,a0.damagedate,a0.damageaddress,a0.reportdate,a0.reportornumber,a2.repairfactorycode,a2.repairfactoryname,\n" +
                "a2.handlername,a2.deflossdate,a2.underwritename,a2.sumverilossfee,a2.sumlossfee,\n" +
                "cast('' as nchar(100)) as checker1,b.handlercode,\n" +
                "cast('' as nchar(100)) as handname,b.handler1code,\n" +
                "cast('' as nchar(100)) as hand1name,\n" +
                "cast('' as nchar(100)) as agentname,a2.underwriteenddate\n" +
                "from  cl_prplregist a0,cl_prpldeflossmain a2,cl_prplcmain b0,cl_prplcitemcar b1,cx_prpcmain b\n" +
                "where substring(a2.underwriteflag,1,1) in ('1','3') \n" +
                "and a2.validflag='1' \n" +
                "and a2.underwriteenddate >= ?\n" +
                "and a2.underwriteenddate <= ?\n" +
                "and a2.registno=b0.registno \n" +
                "and a2.riskcode=b0.riskcode \n" +
                "and b0.id=b1.prplcmainid \n" +
                "and a2.registno=a0.registno\n" +
                "and b0.policyno=b.policyno;",startdate,enddate);
        picczq.update("update songxiu set handname = (select max(username) from utiihandler \n" +
                "where songxiu.handlercode = utiihandler.usercode);");
        picczq.update("update songxiu set checker1 = (select max(checker1) from cl_prplchecktask \n" +
                "where songxiu.registno = cl_prplchecktask.registno);");
        picczq.update("update songxiu set hand1name = (select max(username) from utiihandler \n" +
                "where songxiu.handler1code = usercode);");
        picczq.update("update songxiu set agentname = (select agentname from prpdagent \n" +
                "where songxiu.agentcode = prpdagent.agentcode);");

        List<Map<String, Object>> dataall = picczq.queryForList("select * from songxiu order by underwriteenddate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from songxiu order by underwriteenddate desc limit ?,50;",num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from songxiu order by underwriteenddate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table songxiu;");
        exportToExcel.songxiu1(dataall);
        return new ResultMap3(1, "", true, "", datepage,datenum2);
    }
    public ResultMap3 hesun(String page, String startdate, String enddate){
        int num = Integer.parseInt(page);
        int num3 = (num-1)*50;

        picczq.update("create temporary table temp011_tmp \n" +
                "select a.sumlossfee,a.sumverilossfee,a.cetainlosstype,a.repairfactorytype,a.registno,a.policyno,a.inputtime,a.enddeflossdate, a.finalhandlername,a.underwritecode,\n" +
                "a.underwritename,a.underwriteenddate,a.sumverichgcompfee,a.sumverirepairfee,\n" +
                "(case when left(a.underwriteflag,1) in ('1','3') \n" +
                "then '已核损' else '未核损' end) as  flag1,a.examfactorycode,a.examfactoryname,a.repairfactorycode,a.repairfactoryname,a.prplthirdpartid,0 as reclaim, d.handlercode as reclaimcode, \n" +
                "d.handlername as reclaimname, d.inputtime as reclaiminputtime,d.inputtime as reclaimoutputtime,d.id as reclaimid, a.id as lossmainid, '本地保单' as lflag\n" +
                "from  cl_prpldeflossmain a \n" +
                "inner join cl_prplregistsummary b\n" +
                "on  a.registno = b.registno\n" +
                "and a.riskcode = b.riskcode\n" +
                "and a.enddeflossdate >=?\n" +
                "and a.enddeflossdate <=?\n" +
                "and b.comcode like '4412%' \n" +
                "left join  cl_prplreclaim d \n" +
                "on  a.registno = d.registno\n" +
                "and a.id = d.lossmainid\n" +
                "and d.validflag = '1';",startdate,enddate);
        picczq.update("insert into temp011_tmp\n" +
                "select  a.sumlossfee,a.sumverilossfee,a.cetainlosstype,a.repairfactorytype,a.registno,a.policyno,a.inputtime,a.enddeflossdate,a.finalhandlername,a.underwritecode,\n" +
                "a.underwritename,a.underwriteenddate,a.sumverichgcompfee,a.sumverirepairfee,(case when left(a.underwriteflag,1)  in ('1','3') \n" +
                "then '已核损' else '未核损' end) as  flag1,a.examfactorycode,a.examfactoryname,a.repairfactorycode,a.repairfactoryname,a.prplthirdpartid,0 as reclaim, d.handlercode as reclaimcode, \n" +
                "d.handlername as reclaimname, d.inputtime as reclaiminputtime, d.inputtime as reclaimoutputtime, \n" +
                "d.id as reclaimid, a.id as ossmainid, '外地保单' as lflag\n" +
                "from  cl_prpldeflossmain a \n" +
                "inner join cl_prplregistsummary b\n" +
                "on a.registno = b.registno\n" +
                "and a.riskcode = b.riskcode\n" +
                "and a.enddeflossdate >=?\n" +
                "and a.enddeflossdate <=?\n" +
                "and (b.comcode not like '4412%' and a.makecom like  '4412%')                \n" +
                "left join  cl_prplreclaim d \n" +
                "on  a.registno = d.registno\n" +
                "and a.id = d.lossmainid\n" +
                "and d.validflag = '1';",startdate,enddate);
        picczq.update("create temporary table temp011 \t\n" +
                "select  a.sumlossfee,a.sumverilossfee,a.cetainlosstype,a.repairfactorytype,\n" +
                "cast('' as char(20)) as repairfactorytypename,\n" +
                "a.registno,a.lflag,a.policyno,\n" +
                "a.inputtime,a.enddeflossdate, a.finalhandlername,a.underwritecode,a.underwritename,a.underwriteenddate,a.sumverichgcompfee,a.sumverirepairfee,a.flag1,a.examfactorycode,a.examfactoryname,\n" +
                "a.repairfactorycode,a.repairfactoryname,a.prplthirdpartid,reclaim, reclaimcode,reclaimname, reclaiminputtime, reclaimoutputtime, reclaimid, lossmainid\n" +
                "from temp011_tmp a;");
        picczq.update("update temp011  set repairfactorytypename=(select codecname from prpdcode where codetype = 'RepairFactoryType'\n" +
                "and  validstatus = '1'\n" +
                "and codecode=temp011.repairfactorytype);");
        picczq.update("create index inx_3a01a_01 on temp011(prplthirdpartid);");
        picczq.update("create index inx_3a01a_011 on temp011(lossmainid);");
        picczq.update("create index registno on temp011(registno);");
        picczq.update("create index reclaimid on temp011(reclaimid);");
        picczq.update("update temp011\n" +
                "set reclaiminputtime = ((select max(indate)from  cl_prplbpmmain a\n" +
                "where a.mainno = temp011.registno\n" +
                "and a.businessid = temp011.reclaimid\n" +
                "and a.nodeid = '25')),\n" +
                "reclaimoutputtime = ((select max(outdate)from  cl_prplbpmmain a\n" +
                "where a.mainno = temp011.registno\n" +
                "and a.businessid = temp011.reclaimid\n" +
                "and a.nodeid = '25'));");
        picczq.update("update temp011\n" +
                "set reclaimcode= ((select max(a.usercode) from  cl_prplbpmmain a\n" +
                "where a.mainno = temp011.registno\n" +
                "and a.businessid = temp011.lossmainid\n" +
                "and a.nodeid = '25'\n" +
                "and a.valid = '1')),\n" +
                "reclaiminputtime= ((select max(indate)\n" +
                "from  cl_prplbpmmain a\n" +
                "where a.mainno = temp011.registno\n" +
                "and a.businessid = temp011.lossmainid\n" +
                "and a.nodeid = '25'\n" +
                "and a.valid = '1')),\n" +
                "reclaimoutputtime = ((select  max(outdate)\n" +
                "from  cl_prplbpmmain a\n" +
                "where a.mainno = temp011.registno\n" +
                "and a.businessid = temp011.lossmainid\n" +
                "and a.nodeid = '25'\n" +
                "and a.valid = '1'))\n" +
                "where reclaimcode is null;");
        picczq.update("update temp011\n" +
                "set reclaimname = (select username from prpduser a\n" +
                "where  a.usercode = temp011.reclaimcode);");
        picczq.update("update temp011\n" +
                "set reclaim = 1\n" +
                "where reclaimcode is not null;");
        picczq.update("create index inx_3a01a_02 on temp011(registno)");
        picczq.update("create temporary table temp01\n" +
                "select a.*, b.id ,0 as count1,0 as count2,b.brandname,cast('' as nchar(20)) as flag3,cast('非人伤' as nchar(20)) as flag4,cast('' as nchar(20)) as flag5,\n" +
                "cast('' as nchar(20)) as opname1,\n" +
                "cast('' as nchar(20)) as opname2, \n" +
                "cast('' as nchar(20)) as opname3, \n" +
                "cast('' as nchar(20)) as opname4,\n" +
                "cast('1900-01-01 00:00:00' as datetime) as outdate1, \n" +
                "cast('1900-01-01 00:00:00' as datetime) as outdate2, \n" +
                "cast('1900-01-01' as date) as endcasedate\n" +
                "from temp011 a  left join cl_prpldeflossthirdparty b\n" +
                "on a.registno = b.registno \n" +
                "and a.prplthirdpartid = b.id;");
        picczq.update("create index inx_3a01a_03 on temp01(registno);");
        picczq.update("delete from temp01\n" +
                "where exists (select * from  cl_prplregist reg\n" +
                "where reg.registno = temp01.registno\n" +
                "and reg.cancelflag = '1');");
        picczq.update("create temporary table temp02 \n" +
                "select * from  prpduser\n" +
                "where comcode like '4412%';");
        picczq.update("create index inx_tmp02_01 on temp02(usercode)");
        picczq.update("update temp01 \n" +
                "set opname1 = ifnull((select max(b.username) from  cl_prplcompensate a,temp02 b \n" +
                "where a.registno = temp01.registno\n" +
                "and a.operatorcode = b.usercode\n" +
                "and (a.underwriteflag is null or left(a.underwriteflag,1)  not in('7','8'))), '        ')\n" +
                "where flag1 = '已核损';");
        picczq.update("update temp01\n" +
                "set opname2 = (select max(underwritename) from  cl_prplcompensate a\n" +
                "where a.registno = temp01.registno\n" +
                "and (left(a.underwriteflag,1)  in ('1','3')))\n" +
                "where opname1 is not null;");
        picczq.update("update temp01\n" +
                "set flag3 = '已理算未核赔'\n" +
                "where 0 < (select count(*) from  cl_prplcompensate a\n" +
                "where a.registno = temp01.registno\n" +
                "and (a.underwriteflag is null or left(a.underwriteflag,1) in ('0','2','9')));");
        picczq.update("update temp01\n" +
                "set flag3 = '已理算已核赔'\n" +
                "where 0 < (select count(*) from  cl_prplcompensate a\n" +
                "where a.registno = temp01.registno\n" +
                "and (left(a.underwriteflag,1) in ('1','3')))\n" +
                "and flag3 <> '已理算未核赔';");
        picczq.update("update temp01\n" +
                "set flag3 = '已核损未理算'\n" +
                "where flag1= '已核损' and flag3 not like '已理算%';");
        picczq.update("update temp01\n" +
                "set flag3 = '已定损未核损'\n" +
                "where flag1= '未核损';");
        picczq.update("update temp01\n" +
                "set count1 = (select count(*) from cl_prplcomponent b\n" +
                "where b.prpldeflossmainid = temp01.id\n" +
                "and b.registno = temp01.registno\n" +
                "and b.validflag = '1')\n" +
                "where 1 = 1;");
        picczq.update("update temp01\n" +
                "set count2 = (select count(*) from cl_prplrepairfee b\n" +
                "where b.prpldeflossmainid = temp01.id\n" +
                "and b.registno = temp01.registno\n" +
                "and b.validflag = '1');");
        picczq.update("create index  registno on temp01(registno);");
        picczq.update("update temp01\n" +
                "set flag4 = '人伤案'\n" +
                "where 0 < (select count(*) from  cl_prplbpmmain a\n" +
                "where a.mainno = temp01.registno\n" +
                "and a.nodeid = 10\n" +
                "and a.cancelstate='0'\n" +
                "and a.valid ='1');");
        picczq.update("update temp01\n" +
                "set flag5 = ifnull((select max(a.damagecasename)\n" +
                "from  cl_prplcheck a\n" +
                "where a.registno = temp01.registno),' ');");
        picczq.update("create temporary table temp04 \n" +
                "select a.registno, b.usercode, c.username \n" +
                "from temp01 a inner join  cl_prplbpmmain b\n" +
                "on  a.registno = b.mainno\n" +
                "and  b.nodeid = '14'\n" +
                "and b.valid = 1\n" +
                "left join  temp02 c\n" +
                "on b.usercode = c.usercode;");
        picczq.update("create index registno on temp04(registno);");
        picczq.update("update temp01 \n" +
                "set opname3 = ifnull((select max(a.username)\n" +
                "from temp04 a\n" +
                "where a.registno = temp01.registno),'');");
        picczq.update("create temporary table temp05 \n" +
                "select a.registno, b.usercode, b.outdate, c.username          \n" +
                "from temp01 a inner join cl_prplbpmmain b\n" +
                "on a.registno = b.mainno\n" +
                "and b.nodeid = 2\n" +
                "and b.prepnodeid = 4\n" +
                "and b.valid = 1\n" +
                "left join  temp02 c\n" +
                "on b.usercode = c.usercode\n" +
                "order by b.outdate;");
        picczq.update("create temporary table temp055 \n" +
                "select a.registno, b.usercode, b.outdate, c.username          \n" +
                "from temp01 a inner join cl_prplbpmmain b\n" +
                "on a.registno = b.mainno\n" +
                "and b.nodeid = 2\n" +
                "and b.prepnodeid = 4\n" +
                "and b.valid = 1\n" +
                "left join  temp02 c\n" +
                "on b.usercode = c.usercode\n" +
                "order by b.outdate;");
        picczq.update("create temporary table temp06 \n" +
                "select a1.* from temp05 a1, temp055 a2\n" +
                "where a1.registno = a2.registno\n" +
                "and a1.outdate >= a2.outdate\n" +
                "group by a1.registno, a1.usercode, a1.outdate, a1.username\n" +
                "having count(*) <= 1;");
        picczq.update("create index registno on temp06(registno);");
        picczq.update("update temp01 set opname4=ifnull\n" +
                "((select a.username from temp06 a\n" +
                "where a.registno = temp01.registno),'');");
        picczq.update("update temp01 set outdate1=ifnull((select  a.outdate  from temp06 a\n" +
                "where a.registno = temp01.registno),'1900-01-01 00:00:00');");
        picczq.update("update temp01 set outdate1 = null\n" +
                "where outdate1 = '1900-01-01 00:00:00';");
        picczq.update("create temporary table temp07 \n" +
                "select a.registno, b.outdate \n" +
                "from temp01 a,  cl_prplbpmmain b\n" +
                "where a.registno = b.mainno\n" +
                "and b.nodeid = 16\n" +
                "and b.valid = 1\n" +
                "and left(b.businesstype,1) = 'b'\n" +
                "order by b.outdate desc;");
        picczq.update("create temporary table temp077 \n" +
                "select a.registno, b.outdate \n" +
                "from temp01 a,  cl_prplbpmmain b\n" +
                "where a.registno = b.mainno\n" +
                "and b.nodeid = 16\n" +
                "and b.valid = 1\n" +
                "and left(b.businesstype,1) = 'b'\n" +
                "order by b.outdate desc;");
        picczq.update("create index registno on temp07(registno);");
        picczq.update("create index registno on temp077(registno);");
        picczq.update("create temporary table temp08 \n" +
                "select a1.* from temp07 a1, temp077 a2\n" +
                "where a1.registno = a2.registno\n" +
                "and a1.outdate <= a2.outdate\n" +
                "group by a1.registno, a1.outdate\n" +
                "having count(*) <= 1;");
        picczq.update("create index registno on temp08(registno);");
        picczq.update("update temp01 set outdate2 = ifnull((select a.outdate from temp08 a\n" +
                "where a.registno = temp01.registno),'1900-01-01 00:00:00');");
        picczq.update("update temp01 set outdate2 = null\n" +
                "where outdate2 = '1900-01-01 00:00:00';");
        picczq.update("update temp01\n" +
                "set policyno = (select max(policyno) from  cl_prplregistsummary a\n" +
                "where a.registno = temp01.registno)\n" +
                "where policyno is null;");
        picczq.update("create temporary table temp09 \n" +
                "select a.registno, b.endcasedate\n" +
                "from temp01 a,  cl_prplclaim b\n" +
                "where a.registno = b.registno\n" +
                "order by endcasedate desc;");
        picczq.update("create temporary table temp099 \n" +
                "select a.registno, b.endcasedate\n" +
                "from temp01 a,  cl_prplclaim b\n" +
                "where a.registno = b.registno\n" +
                "order by endcasedate desc;");
        picczq.update("create index registno on temp09(registno);");
        picczq.update("create index registno on temp099(registno);");
        picczq.update("create temporary table temp10 \n" +
                "select a1.* from temp09 a1, temp099 a2\n" +
                "where a1.registno = a2.registno\n" +
                "and a1.endcasedate <= a2.endcasedate\n" +
                "group by a1.registno, a1.endcasedate\n" +
                "having count(*) <= 1;");
        picczq.update("create index registno on temp10(registno);");
        picczq.update("update temp01 set endcasedate = ifnull((select a.endcasedate from temp10 a\n" +
                "where a.registno = temp01.registno),'1900-01-01 00:00:00');");
        picczq.update("update temp01 set endcasedate = null\n" +
                "where endcasedate = '1900-01-01';");
        picczq.update("update temp01 set flag3 = '已结案'\n" +
                "where exists (select * from  cl_prplclaim lclaim\n" +
                "where lclaim.registno = temp01.registno\n" +
                "and lclaim.endcasedate is not null);");
        picczq.update("create index  policyno on temp01(policyno);");
        picczq.update("create temporary table temp11 \n" +
                "select  a.*,b.agentcode,left(b.comcode,10) as comcode,b.handler1code,b.insuredcode,b.insuredname,left(a.brandname,4) as brandname1,\n" +
                "b.startdate,b.enddate, e.damagedate, e.reportdate, e.reportornumber,f.monopolycode, f.monopolyname\n" +
                "from temp01 a left join ( cl_prplcmain b  left join cx_prpcmain g\n" +
                "on  b.policyno = g.policyno left join cx_prpcitem_car f\n" +
                "on g.proposalno = f.proposalno)\n" +
                "on a.policyno = b.policyno\n" +
                "and a.registno = b.registno \n" +
                "left join cl_prplregist e\n" +
                "on a.registno = e.registno;");
        picczq.update("create index   registno on  temp11(registno); ");
        picczq.update("create temporary table temp12 \n" +
                "select a.*, b.checknature, c.checker1, c.checker2 \n" +
                "from temp11 a  left join cl_prplcheck b\n" +
                "on a.registno = b.registno\n" +
                "and b.validflag = '1'\n" +
                "left join  cl_prplchecktask c\n" +
                "on a.registno = c.registno\n" +
                "and c.validflag = '1';");
        picczq.update("create temporary table temp13 \t\n" +
                "select a.*,cast('' as nchar(20)) as licenseno0,cast('' as nchar(100)) as brandname0,cast('' as nchar(20)) as useyears0\n" +
                "from temp12 a;");

        picczq.update("create index registno on temp13(registno);");
        picczq.update("update temp13 set licenseno0 = (select max(licenseno) from cl_prplcitemcar\n" +
                "where temp13.registno = cl_prplcitemcar.registno);");
        picczq.update("update temp13 set brandname0 = (select max(brandname) from cl_prplcitemcar\n" +
                "where temp13.registno = cl_prplcitemcar.registno);");
        picczq.update("update temp13 set useyears0 = (select max(useyears) from cl_prplcitemcar\n" +
                "where temp13.registno = cl_prplcitemcar.registno);");


        List<Map<String, Object>> dataall = picczq.queryForList("select * from temp13 order by enddeflossdate desc;");
        List<Map<String, Object>> datepage = picczq.queryForList("select * from temp13 order by enddeflossdate desc limit ?,50;",num3);
        List<Map<String, Object>> datenum = picczq.queryForList("select count(*) as num from temp13 order by enddeflossdate desc;");
        String datenum1 = datenum.get(0).get("num").toString();
        int datenum2 = Integer.parseInt(datenum1);
        picczq.update("drop table temp011_tmp,temp011,temp01,temp02,temp04,temp055,temp05,temp06,temp077,temp07,temp08,temp09,temp099,temp10,temp11,temp12,temp13;");
        exportToExcel.hesun1(dataall);
        return new ResultMap3(1, "", true, "", datepage,datenum2);
    }

}

 