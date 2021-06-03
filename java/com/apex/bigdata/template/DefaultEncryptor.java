package com.apex.bigdata.template;

import org.apache.commons.lang.StringUtils;
import org.jasypt.encryption.StringEncryptor;

public class DefaultEncryptor implements StringEncryptor {
    private static final char[][] s = new char[][]{{'@', '1', '0', '0', '0', '0', '0', '1'}, {'!', '1', '1', '1', '0', '0', '0', '0'}, {'1', '1', '0', '0', '1', '0', '$', '1'}, {'$', '1', '1', '1', '1', '0', '0', '0'}, {'1', '0', '%', '1', '1', '1', '1', '0'}, {'!', '1', '0', '1', '0', '1', '1', '1'}, {'~', '1', '1', '1', '0', '1', '0', '0'}, {')', '1', '0', '1', '1', '1', '1', '0'}, {'&', '(', '1', '1', '0', '0', '1', '0'}, {'&', '1', '1', '0', '0', '*', '0', '0'}, {'?', '1', '1', '0', '0', '0', '1', '}'}, {'{', '1', '-', '1', '1', '0', '0', '0'}};
    private static DefaultEncryptor defaultEncryptor = new DefaultEncryptor();
    public DefaultEncryptor() {
    }

    /**
     * 加密
     * @param message
     * @return
     */
    public String encrypt(String message) {
        return DesEncrypt.strEncode(message, new String[]{this.getS(), this.getS()});
    }

    /**
     * 解密
     * @param encryptedMessage
     * @return
     */
    public String decrypt(String encryptedMessage) {
        return DesEncrypt.strDecode(encryptedMessage, new String[]{this.getS(), this.getS()});
    }

    private String toBinary(String str) {
        char[] strChar = str.toCharArray();
        String result = "";

        for (int i = 0; i < strChar.length; ++i) {
            result = result + Integer.toBinaryString(strChar[i]) + "\n";
        }

        return result;
    }

    private int[] BinstrToIntArray(String binStr) {
        char[] temp = binStr.toCharArray();
        int[] result = new int[temp.length];

        for (int i = 0; i < temp.length; ++i) {
            result[i] = temp[i] - 48;
        }

        return result;
    }

    private char BinstrToChar(String binStr) {
        int[] temp = this.BinstrToIntArray(binStr);
        int sum = 0;

        for (int i = 0; i < temp.length; ++i) {
            sum += temp[temp.length - 1 - i] << i;
        }

        return (char) sum;
    }

    private String toStr(String str) {
        String[] tempStr = str.split("\n");
        char[] tempChar = new char[tempStr.length];

        for (int i = 0; i < tempStr.length; ++i) {
            tempChar[i] = this.BinstrToChar(tempStr[i]);
        }

        return String.valueOf(tempChar);
    }

    private String getS() {
        StringBuilder str = new StringBuilder();

        for (int i = 0; i < s.length; ++i) {
            for (int j = 0; j < s[i].length; ++j) {
                if (s[i][j] == '1' || s[i][j] == '0') {
                    str.append(s[i][j]);
                }
            }

            str.append('\n');
        }

        return str.toString();
    }

    public static String parsePassword(String password){
        try{
            if(!StringUtils.isBlank(password) && (password.startsWith("ENC(") && password.endsWith(")"))){
                defaultEncryptor  = new DefaultEncryptor();
                return defaultEncryptor.decrypt(password.substring(4,password.length()-1));
            }
            return password;
        }catch (Exception e){
            return password;
        }
    }

//    public static void main(String[] args) {
//        DefaultEncryptor defaultEncryptor = new DefaultEncryptor();
//        System.out.println(defaultEncryptor.encrypt("apexsoft"));
//        System.out.println(defaultEncryptor.encrypt("Apex@123"));
//        System.out.println(defaultEncryptor.decrypt("D0BF04496B316834949C5E304557691315BF9B7D4FA02D75"));
//        System.out.println(defaultEncryptor.decrypt(parsePassword("1083061FF4033F3397FED9C2AF4AED9F")));
//        System.out.println(parsePassword("ENC(1083061FF4033F3397FED9C2AF4AED9F)"));
//
//    }
}
