package com.example.etl.domain;



/**
 * Created by wangweirong05 on 2017-09-15.
 */
public class ResultMap<T> {

    private Integer code;
    private String message;
    private Boolean success;
    private T data;
    private T data2;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }




    public T getData2() {
        return data2;
    }

    public void setData2(T data2) {
        this.data2 = data2;
    }

    public ResultMap(Integer code, String message, Boolean success, T data,T data2) {
        this.code = code;
        this.message = message;
        this.success = success;
        this.data = data;
        this.data2 = data2;
    }
}
