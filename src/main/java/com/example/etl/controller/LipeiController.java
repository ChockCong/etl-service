package com.example.etl.controller;


import com.example.etl.domain.ResultMap;
import com.example.etl.domain.ResultMap3;
import com.example.etl.services.Lipei;
import com.example.etl.services.Login;
import javafx.scene.input.DataFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;
import javax.xml.transform.Result;
import java.time.DateTimeException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

@Component
@RestController
@RequestMapping(value = "/lipei")
public class LipeiController {

    @Autowired
    protected Lipei lipei;

    @PostMapping(value = "/baoan")
    public ResultMap3 baoan(HttpSession session,
                           @RequestParam(required = true) String page,
                           @RequestParam(required = true) String startdate,
                           @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = lipei.baoan(page,startdate,enddate);
//        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }

    @PostMapping(value = "/jiean")
    public ResultMap3 jiean(HttpSession session,
                            @RequestParam(required = true) String page,
                            @RequestParam(required = true) String startdate,
                            @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = lipei.jieban(page,startdate,enddate);
//        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
    @PostMapping(value = "/songxiu")
    public ResultMap3 songxiu(HttpSession session,
                            @RequestParam(required = true) String page,
                            @RequestParam(required = true) String startdate,
                            @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = lipei.songxiu(page,startdate,enddate);
//        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }

    @PostMapping(value = "/hesun")
    public ResultMap3 hesun(HttpSession session,
                              @RequestParam(required = true) String page,
                              @RequestParam(required = true) String startdate,
                              @RequestParam(required = true) String enddate

    ){
        ResultMap3 result = lipei.hesun(page,startdate,enddate);
//        Object dataall = result.getData();
        Object datepage = result.getData2();
        Object datenum = result.getData3();
        return new ResultMap3(1, "", true, "", datepage,datenum);
    }
}

 