package com.example.etl.controller;



import com.example.etl.domain.ResultMap;
import com.example.etl.utils.DateTimeUtils;
import com.example.etl.utils.UploadFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;


import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpSession;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@Component
@RestController
@RequestMapping(value = "/test")
public class  TestController {
//    @Autowired
//    private MailService mailService;
@Autowired
@Qualifier("picczq")
protected JdbcTemplate picczq;

    //收件人
    private String[] test ={"picczq14@picczq.com"};
    //肇庆
    private String[] zq ={"picczq14@picczq.com","kongyuji@picczq.com","chenjianqiang@picczq.com","ouwanchao@picczq.com",
            "liangzhijian@picczq.com","linyi@picczq.com","chenshishen@picczq.com","liangguoyi@picczq.com","wangwei@picczq.com"};
    //    鼎湖
    private String[] dh ={"picczq14@picczq.com","shaozuhui@picczq.com"};
    //    广宁
    private String[] gn ={"picczq14@picczq.com","huangjianjun@picczq.com"};
    //    四会
    private String[] sh ={"picczq14@picczq.com","liangguoqian@picczq.com"};
    //    怀集
    private String[] hj ={"picczq14@picczq.com","chensi@picczq.com","liangweituan@picczq.com"};
    //    高要
    private String[] gy ={"picczq14@picczq.com","tanshu@picczq.com"};
    //    德庆
    private String[] dq ={"picczq14@picczq.com","huangweiming@picczq.com"};
    //    封开
    private String[] fk ={"picczq14@picczq.com","xuyunyong@picczq.com"};
    //    端州
    private String[] dz ={"picczq14@picczq.com","zhanglan@picczq.com"};
    //    车商
    private String[] cs ={"picczq14@picczq.com","ouyaohao@picczq.com"};
    //    黄凌志
    private String[] huanglingzhi ={"huanglingzhi@picczq.com","picczq14@picczq.com"};
    //    符嘉敏
    private String[] fujiamin ={"fujiamin@picczq.com","picczq14@picczq.com"};
    //    出单中心
    private String[] cdtj ={"konghao@picczq.com","limeizhuang@picczq.com","picczq14@picczq.com"};
    //    黄玉芳
    private String[] huangyufang = {"huangyufang@picczq.com","picczq14@picczq.com"};

   int monthminonenum = DateTimeUtils.getmonthminonenum();

//    @Autowired
//    private BillFile billFile;
    static {
        System.setProperty("etl.mime.splitlongparameters","false");
    }
    @GetMapping("/sendMail")
    public ResultMap sendMail(){
        String todayminone = DateTimeUtils.getdateminone();
//        String[] zqfile = {"D:\\bill\\肇庆-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\肇庆-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\肇庆-" + monthminonenum + "月核损清单.xls","D:\\bill\\肇庆-" + monthminonenum + "月结案清单.xls"};
//        String[] dhfile = {"D:\\bill\\鼎湖-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\鼎湖-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\鼎湖-" + monthminonenum + "月核损清单.xls","D:\\bill\\鼎湖-" + monthminonenum + "月结案清单.xls"};
//        String[] gnfile = {"D:\\bill\\广宁-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\广宁-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\广宁-" + monthminonenum + "月核损清单.xls","D:\\bill\\广宁-" + monthminonenum + "月结案清单.xls"};
//        String[] shfile = {"D:\\bill\\四会-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\四会-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\四会-" + monthminonenum + "月核损清单.xls","D:\\bill\\四会-" + monthminonenum + "月结案清单.xls"};
//        String[] hjfile = {"D:\\bill\\怀集-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\怀集-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\怀集-" + monthminonenum + "月核损清单.xls","D:\\bill\\怀集-" + monthminonenum + "月结案清单.xls"};
//        String[] gyfile = {"D:\\bill\\高要-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\高要-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\高要-" + monthminonenum + "月核损清单.xls","D:\\bill\\高要-" + monthminonenum + "月结案清单.xls"};
//        String[] dqfile = {"D:\\bill\\德庆-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\德庆-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\德庆-" + monthminonenum + "月核损清单.xls","D:\\bill\\德庆-" + monthminonenum + "月结案清单.xls"};
//        String[] fkfile = {"D:\\bill\\封开-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\封开-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\封开-" + monthminonenum + "月核损清单.xls","D:\\bill\\封开-" + monthminonenum + "月结案清单.xls"};
//        String[] dzfile = {"D:\\bill\\端州-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\端州-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\端州-" + monthminonenum + "月核损清单.xls","D:\\bill\\端州-" + monthminonenum + "月结案清单.xls"};
//        String[] csfile = {"D:\\bill\\车商-"+  monthminonenum+  "月报案清单.xls","D:\\bill\\车商-" + monthminonenum + "月送修清单.xls",
//                           "D:\\bill\\车商-" + monthminonenum + "月核损清单.xls","D:\\bill\\车商-" + monthminonenum + "月结案清单.xls"};


        String[] zqfile = {".//data//肇庆-"+  monthminonenum+  "月报案清单.xls",".//data//肇庆-" + monthminonenum + "月送修清单.xls",
                ".//data//肇庆-" + monthminonenum + "月核损清单.xls",".//data//肇庆-" + monthminonenum + "月结案清单.xls"};
        String[] dhfile = {".//data//鼎湖-"+  monthminonenum+  "月报案清单.xls",".//data//鼎湖-" + monthminonenum + "月送修清单.xls",
                ".//data//鼎湖-" + monthminonenum + "月核损清单.xls",".//data//鼎湖-" + monthminonenum + "月结案清单.xls"};
        String[] gnfile = {".//data//广宁-"+  monthminonenum+  "月报案清单.xls",".//data//广宁-" + monthminonenum + "月送修清单.xls",
                ".//data//广宁-" + monthminonenum + "月核损清单.xls",".//data//广宁-" + monthminonenum + "月结案清单.xls"};
        String[] shfile = {".//data//四会-"+  monthminonenum+  "月报案清单.xls",".//data//四会-" + monthminonenum + "月送修清单.xls",
                ".//data//四会-" + monthminonenum + "月核损清单.xls",".//data//四会-" + monthminonenum + "月结案清单.xls"};
        String[] hjfile = {".//data//怀集-"+  monthminonenum+  "月报案清单.xls",".//data//怀集-" + monthminonenum + "月送修清单.xls",
                ".//data//怀集-" + monthminonenum + "月核损清单.xls",".//data//怀集-" + monthminonenum + "月结案清单.xls"};
        String[] gyfile = {".//data//高要-"+  monthminonenum+  "月报案清单.xls",".//data//高要-" + monthminonenum + "月送修清单.xls",
                ".//data//高要-" + monthminonenum + "月核损清单.xls",".//data//高要-" + monthminonenum + "月结案清单.xls"};
        String[] dqfile = {".//data//德庆-"+  monthminonenum+  "月报案清单.xls",".//data//德庆-" + monthminonenum + "月送修清单.xls",
                ".//data//德庆-" + monthminonenum + "月核损清单.xls",".//data//德庆-" + monthminonenum + "月结案清单.xls"};
        String[] fkfile = {".//data//封开-"+  monthminonenum+  "月报案清单.xls",".//data//封开-" + monthminonenum + "月送修清单.xls",
                ".//data//封开-" + monthminonenum + "月核损清单.xls",".//data//封开-" + monthminonenum + "月结案清单.xls"};
        String[] dzfile = {".//data//端州-"+  monthminonenum+  "月报案清单.xls",".//data//端州-" + monthminonenum + "月送修清单.xls",
                ".//data//端州-" + monthminonenum + "月核损清单.xls",".//data//端州-" + monthminonenum + "月结案清单.xls"};
        String[] csfile = {".//data//车商-"+  monthminonenum+  "月报案清单.xls",".//data//车商-" + monthminonenum + "月送修清单.xls",
                ".//data//车商-" + monthminonenum + "月核损清单.xls",".//data//车商-" + monthminonenum + "月结案清单.xls"};





        String title1 = monthminonenum+"月肇庆月度清单";
        String title2 = monthminonenum+"月鼎湖月度清单";
        String title3 = monthminonenum+"月广宁月度清单";
        String title4 = monthminonenum+"月四会月度清单";
        String title5 = monthminonenum+"月怀集月度清单";
        String title6 = monthminonenum+"月高要月度清单";
        String title7 = monthminonenum+"月德庆月度清单";
        String title8 = monthminonenum+"月封开月度清单";
        String title9 = monthminonenum+"月端州月度清单";
        String title10 = monthminonenum+"月车商月度清单";
        String content ="报案清单、送修清单、核损清单、结案清单";
//        ResultMap baoan = billFile.monthbaoan();
//        ResultMap songxiu = billFile.monthsongxiu();
//        ResultMap hesun = billFile.monthhesun();
//        ResultMap jiean = billFile.monthjiean();
//
//
//
//        try {
//            mailService.sendAttachmentsMail(zq, title1, content, zqfile);
//            mailService.sendAttachmentsMail(dh, title2, content, dhfile);
//            mailService.sendAttachmentsMail(gn, title3, content, gnfile);
//            mailService.sendAttachmentsMail(sh, title4, content, shfile);
//            mailService.sendAttachmentsMail(hj, title5, content, hjfile);
//            mailService.sendAttachmentsMail(gy, title6, content, gyfile);
//            mailService.sendAttachmentsMail(dq, title7, content, dqfile);
//            mailService.sendAttachmentsMail(fk, title8, content, fkfile);
//            mailService.sendAttachmentsMail(dz, title9, content, dzfile);
//            mailService.sendAttachmentsMail(cs, title10, content, csfile);
//        } catch (MessagingException e) {
//            return null;
//        } catch (UnsupportedEncodingException e) {
//
//        }




        return new ResultMap(1,"",true,"","");
    }



    @RequestMapping(value="/testuploadimg", method = RequestMethod.POST)
    public @ResponseBody String uploadImg(@RequestParam("file") MultipartFile file,
                                          HttpServletRequest request) {
        String contentType = file.getContentType();
        String fileName = file.getOriginalFilename();
        /*System.out.println("fileName-->" + fileName);
        System.out.println("getContentType-->" + contentType);*/
//        String filePath = request.getSession().getServletContext().getRealPath("imgupload/");
        String filePath = "\\\\D:\\bill\\";

        try {
            UploadFile.upload(file.getBytes(), filePath, fileName);
        } catch (Exception e) {
            // TODO: handle exception
        }
        //返回json
        return "uploadimg success";
    }

       @GetMapping("/ajax")
    public  ResultMap ajax(){

           List<Map<String, Object>> data = picczq.queryForList("select * from ajax ");

           return new ResultMap(1,"",true,data,"");

       }
}