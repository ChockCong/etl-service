package com.example.etl.controller;

import com.example.etl.domain.ResultMap3;
import com.example.etl.services.Xiaoguan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;

@Component
@RestController

@RequestMapping(value = "/xiaoguan")

public class XiaoguanController {

    @Autowired
    protected Xiaoguan xiaoguan;

    @PostMapping(value = "/cheshang")
    public ResultMap3 cheshang(HttpSession session,
                            @RequestParam(required = true) String page,
                            @RequestParam(required = true) String startdate,
                            @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.cheshang(page,startdate,enddate);
//        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/chexianqibao")
    public ResultMap3 chexianqibao(HttpSession session,
                            @RequestParam(required = true) String page,
                            @RequestParam(required = true) String startdate,
                            @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.chexianqibao(page,startdate,enddate);
        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/chexianjiaochaxiaoshou")
    public ResultMap3 chexianjiaochaxiaoshou(HttpSession session,
                                   @RequestParam(required = true) String page,
                                   @RequestParam(required = true) String startdate,
                                   @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.chexianjiaochaxiaoshou(page,startdate,enddate);
        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/chexianqiandan")
    public ResultMap3 chexianqiandan(HttpSession session,
                                             @RequestParam(required = true) String page,
                                             @RequestParam(required = true) String startdate,
                                             @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.chexianqiandan(page,startdate,enddate);
        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/feicheqibao")
    public ResultMap3 feicheqibao(HttpSession session,
                                     @RequestParam(required = true) String page,
                                     @RequestParam(required = true) String startdate,
                                     @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.feicheqibao(page,startdate,enddate);
        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/feicheqiandan")
    public ResultMap3 feicheqiandan(HttpSession session,
                                  @RequestParam(required = true) String page,
                                  @RequestParam(required = true) String startdate,
                                  @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = xiaoguan.feicheqiandan(page,startdate,enddate);
        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/feichejiaochaxiaoshou")
    public ResultMap3 feichejiaochaxiaoshou(HttpSession session,
                                    @RequestParam(required = true) String page,
                                    @RequestParam(required = true) String startdate,
                                    @RequestParam(required = true) String enddate

    ) {
            ResultMap3 result = xiaoguan.feichejiaochaxiaoshou(page,startdate,enddate);
            Object datepage = result.getData2();
            Object datenum = result.getData3();
            return new ResultMap3(1, "", true, "", datepage,datenum);
        }
}

 