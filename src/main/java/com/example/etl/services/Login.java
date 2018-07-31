package com.example.etl.services;

import com.example.etl.domain.ResultMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class Login {
    @Autowired
    @Qualifier("picczq")
    protected JdbcTemplate picczq;
    public ResultMap check(String gh,String password){
        List<Map<String, Object>> num = picczq.queryForList("select count(gh) as num from user where gh= ?",gh);
        String num1 = num.get(0).get("num").toString();
         int num2 = Integer.parseInt(num1);
         if (num2 == 1){
//             List<Map<String, Object>> passwordarry = picczq.queryForList("select password from user where id =?",id);
//             String password2 = passwordarry.get(0).get("password").toString();
             List<Map<String, Object>> result = picczq.queryForList("SELECT IF((select password from user where gh= ?)=md5(?),true,false) as result",gh,password);
             String result1 = result.get(0).get("result").toString();
             int result2 = Integer.parseInt(result1);
             if (result2==1){
                 List<Map<String, Object>> namearry = picczq.queryForList("select name,depflag from user where gh = ?",gh);
                 String name = namearry.get(0).get("name").toString();
                 String depflag = namearry.get(0).get("depflag").toString();
                 return new ResultMap(1, "", true, name, depflag);
             }else{
                 return new ResultMap(0, "", true, "", "");
             }
         }else{
             return new ResultMap(0, "", true, "", "");
         }
    }
    public ResultMap alterpassword(String gh,String password){
        int a = picczq.update("update user set password = md5(?) where gh= ?",password,gh);
        if (a==1){
            return new ResultMap(1, "", true, "", "");
        }else {
            return new ResultMap(0, "", true, "", "");
        }
    }
}

 