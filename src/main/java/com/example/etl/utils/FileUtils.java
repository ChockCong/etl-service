package com.example.etl.utils;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

/**
 * @author malongbo
 */
public final class FileUtils {

    /**
     * 获取文件扩展名*
     * @param fileName 文件名
     * @return 扩展名
     */
    public static String getExtension(String fileName) {
        int i = fileName.lastIndexOf(".");
        if (i < 0) return null;

        return fileName.substring(i+1);
    }

    /**
     * 获取文件扩展名*
     * @param file 文件对象
     * @return 扩展名
     */
    public static String getExtension(File file) {
        if (file == null) return null;

        if (file.isDirectory()) return null;

        String fileName = file.getName();
        return getExtension(fileName);
    }

    /**
     * 读取文件*
     * @param filePath 文件路径
     * @return 文件对象
     */
    public static File readFile(String filePath) {
        File file = new File(filePath);
        if (file.isDirectory()) return null;

        if (!file.exists()) return null;

        return file;
    }
    /**
     * 复制文件
     * @param oldFilePath 源文件路径
     * @param newFilePath 目标文件毒经
     * @return 是否成功
     */
    public static boolean copyFile(String oldFilePath,String newFilePath) {
        try {
            int byteRead = 0;
            File oldFile = new File(oldFilePath);
            if (oldFile.exists()) { // 文件存在时
                InputStream inStream = new FileInputStream(oldFilePath); // 读入原文件
                FileOutputStream fs = new FileOutputStream(newFilePath);
                byte[] buffer = new byte[1444];
                while ((byteRead = inStream.read(buffer)) != -1) {
                    fs.write(buffer, 0, byteRead);
                }
                inStream.close();
                fs.close();
                return true;
            }
            return false;
        } catch (Exception e) {
            System.out.println("复制单个文件操作出错 ");
            e.printStackTrace();
           return false;
        }
    }

    /**
     *删除文件
     * @param filePath 文件地址
     * @return 是否成功
     */
    public static boolean delFile(String filePath) {
        return delFile(new File(filePath));
    }

    /**
     * 删除文件
     * @param file 文件对象
     * @return 是否成功
     */
    public static boolean delFile(File file) {
        if (file.exists()) {
            return file.delete();
        }
        return false;
    }

    /**
     * png图片转jpg*
     * @param pngImage png图片对象
     * @param jpegFile jpg图片对象
     * @return 转换是否成功
     */
    public static boolean png2jpeg(File pngImage, File jpegFile) {
        BufferedImage bufferedImage;

        try {
            bufferedImage = ImageIO.read(pngImage);

            BufferedImage newBufferedImage = new BufferedImage(bufferedImage.getWidth(),
                    bufferedImage.getHeight(), BufferedImage.TYPE_INT_RGB);

            newBufferedImage.createGraphics().drawImage(bufferedImage, 0, 0, Color.WHITE, null);

            ImageIO.write(bufferedImage, "jpg", jpegFile);

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 判断文件是否是图片*
     * @param imgFile 文件对象
     * @return
     */
    public static boolean isImage(File imgFile) {
        try {
            BufferedImage image = ImageIO.read(imgFile);
            return image != null;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 根据系统当前时间，返回时间层次的文件夹结果，如：upload/2015/01/18/0.jpg
     * @return
     */
    public static String getTimeFilePath(){
    	return new SimpleDateFormat("/yyyy/MM/dd").format(new Date())+"/";
    }

    /**
     * 将文件头转换成16进制字符串
     *
     * @param src 原生byte
     * @return 16进制字符串
     */
    private static String bytesToHexString(byte[] src){

        StringBuilder stringBuilder = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    /**
     * 得到文件头
     *
     * @param file 文件
     * @return 文件头
     * @throws IOException
     */
    private static String getFileContent(File file) throws IOException {

        byte[] b = new byte[28];

        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream(file);
            inputStream.read(b, 0, 28);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
        return bytesToHexString(b);
    }



//每天清单
    public static final String getTitle(String baobiao){
        String FILEPATH = ".//data//";
//        String FILEPATH = "\\\\D:\\bill\\";
        String title=FILEPATH+DateTimeUtils.getdateminone()+"-"+baobiao+".xls";
        return title;
    }
//月份清单
    public static final String getTitle2(String baobiao2){
        //linux 路径
        String FILEPATH = ".//data//";
        //windows 路径
//        String FILEPATH = "\\\\D:\\bill\\";
        String title=FILEPATH+baobiao2+".xls";
        return title;
    }

    public static final File saveExcelFile(ArrayList<String> keywords,Map<String, String> headData, String title, File file, List<Map<String, Object>> datas) {
        // 创建工作薄
        HSSFWorkbook hssfWorkbook = new HSSFWorkbook();
        HSSFSheet hssfSheet = hssfWorkbook.createSheet();
        HSSFRow row = hssfSheet.createRow(0);
        HSSFCell cell = null;
        int rowIndex = 0;
        int cellIndex = 0;

        row = hssfSheet.createRow(rowIndex);
        rowIndex++;

        cell = row.createCell(cellIndex);
        cell.setCellValue(title);

        row = hssfSheet.createRow(rowIndex);
        rowIndex++;

        for (String h : keywords) {
            //创建列
            cell = row.createCell(cellIndex);
            //索引递增
            cellIndex++;
            //逐列插入标题
            cell.setCellValue(headData.get(h));
        }
        if (datas != null) {
            // 获取所有的记录 有多少条记录就创建多少行
            for (int i = 0; i < datas.size(); i++) {
                row = hssfSheet.createRow(rowIndex);
                // 得到所有的行 一个record就代表 一行
                Map<String, Object> record = new HashMap<>();
                record = datas.get(i);
                //下一行索引
                rowIndex++;
                //刷新新行索引
                cellIndex = 0;
                // 在有所有的记录基础之上，便利传入进来的表头,再创建N行
                for (String h : keywords) {
                    cell = row.createCell(cellIndex);
                    cellIndex++;
                    //按照每条记录匹配数据
                    cell.setCellValue(record.get(h) == null ? "" : record.get(h).toString());
                }
            }
        }
        try {
            FileOutputStream fileOutputStreane = new FileOutputStream(file);
            hssfWorkbook.write(fileOutputStreane);
            fileOutputStreane.flush();
            fileOutputStreane.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }
}





