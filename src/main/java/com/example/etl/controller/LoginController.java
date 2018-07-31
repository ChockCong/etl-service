package com.example.etl.controller;

import com.example.etl.domain.ResultMap;
import com.example.etl.services.Login;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;

@Component
@RestController
@RequestMapping(value = "/login")
public class LoginController {

    @Autowired
    protected Login login;


    @PostMapping(value = "/check")
    public ResultMap register(HttpSession session,
                              @RequestParam(required = true) String gh,
                              @RequestParam(required = true) String password
    ){

        ResultMap result = login.check(gh,password);
        int code = result.getCode();
        String name = (String) result.getData();
        String adflag = (String) result.getData2();
        if (code == 1){
            return new ResultMap(1, "登陆成功！", true, name, adflag);
        }else{
            return new ResultMap(0, "登陆失败！", true, "", "");
        }
    }
    @PostMapping(value = "/alterpassword")
    public ResultMap alterpassword(HttpSession session,
                              @RequestParam(required = true) String gh,
                              @RequestParam(required = true) String password
    ){

        ResultMap result = login.alterpassword(gh,password);
        int code = result.getCode();
        if (code==1){
            return new ResultMap(1, "", true, "", "");

        }else{
            return new ResultMap(0, "", true, "", "");

        }
    }

}

 