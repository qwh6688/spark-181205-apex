package com.apex.bigdata.template;

import java.util.ArrayList;
import java.util.List;

public final class DesEncrypt {
    public DesEncrypt() {
    }

    public static void main(String[] args) {
        String str = "root123";
        str = strEncode(str, "apexsoft");
        System.out.println(str);
        str = strDecode("L5WIuZlV3ZMUZcAF81BMXQ==", "apexsoft");
        System.out.println(str);
    }

    public static String strEncode(String data, String... keys) {
        int key_length = null == keys ? 0 : keys.length;
        String firstKey = key_length > 0 ? keys[0] : null;
        String secondKey = key_length > 1 ? keys[1] : null;
        String thirdKey = key_length > 2 ? keys[2] : null;
        int leng = data.length();
        List firstKeyBt = null;
        List secondKeyBt = null;
        List thirdKeyBt = null;
        String encData = "";
        int firstLength = 0;
        int secondLength = 0;
        int thirdLength = 0;
        if (firstKey != null && firstKey != "") {
            firstKeyBt = getKeyBytes(firstKey);
            firstLength = firstKeyBt.size();
        }

        if (secondKey != null && secondKey != "") {
            secondKeyBt = getKeyBytes(secondKey);
            secondLength = secondKeyBt.size();
        }

        if (thirdKey != null && thirdKey != "") {
            thirdKeyBt = getKeyBytes(thirdKey);
            thirdLength = thirdKeyBt.size();
        }

        if (leng > 0) {
            if (leng < 4) {
                int[] bt = strToBt(data);
                int[] encByte = null;
                int[] tempBt;
                boolean x;
                boolean y;
                int x1;
                int y1;
                if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "" && thirdKey != null && thirdKey != "") {
                    x = false;
                    y = false;
                    boolean z1 = false;
                    tempBt = bt;

                    for(x1 = 0; x1 < firstLength; ++x1) {
                        tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                    }

                    for(y1 = 0; y1 < secondLength; ++y1) {
                        tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                    }

                    for(int z = 0; z < thirdLength; ++z) {
                        tempBt = enc(tempBt, (int[])((int[])thirdKeyBt.get(z)));
                    }

                    encByte = tempBt;
                } else if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "") {
                    x = false;
                    y = false;
                    tempBt = bt;

                    for(x1 = 0; x1 < firstLength; ++x1) {
                        tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                    }

                    for(y1 = 0; y1 < secondLength; ++y1) {
                        tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                    }

                    encByte = tempBt;
                } else if (firstKey != null && firstKey != "") {
                    x = false;
                    tempBt = bt;

                    for(x1 = 0; x1 < firstLength; ++x1) {
                        tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                    }

                    encByte = tempBt;
                }

                encData = bt64ToHex(encByte);
            } else {
                int iterator = leng / 4;
                int remainder = leng % 4;
                boolean i1 = false;

                int[] tempBt;
                boolean x;
                boolean y;
                boolean z;
                String remainderData;
                int[] tempByte;
                int[] encByte;
                int x1;
                int y1;
                int z1;
                for(int i = 0; i < iterator; ++i) {
                    remainderData = data.substring(i * 4 + 0, i * 4 + 4);
                    tempByte = strToBt(remainderData);
                    encByte = null;
                    if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "" && thirdKey != null && thirdKey != "") {
                        x = false;
                        y = false;
                        z = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        for(y1 = 0; y1 < secondLength; ++y1) {
                            tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                        }

                        for(z1 = 0; z1 < thirdLength; ++z1) {
                            tempBt = enc(tempBt, (int[])((int[])thirdKeyBt.get(z1)));
                        }

                        encByte = tempBt;
                    } else if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "") {
                        x = false;
                        y = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        for(y1 = 0; y1 < secondLength; ++y1) {
                            tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                        }

                        encByte = tempBt;
                    } else if (firstKey != null && firstKey != "") {
                        x = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        encByte = tempBt;
                    }

                    encData = encData + bt64ToHex(encByte);
                }

                if (remainder > 0) {
                    remainderData = data.substring(iterator * 4 + 0, leng);
                    tempByte = strToBt(remainderData);
                    encByte = null;
                    if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "" && thirdKey != null && thirdKey != "") {
                        x = false;
                        y = false;
                        z = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        for(y1 = 0; y1 < secondLength; ++y1) {
                            tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                        }

                        for(z1 = 0; z1 < thirdLength; ++z1) {
                            tempBt = enc(tempBt, (int[])((int[])thirdKeyBt.get(z1)));
                        }

                        encByte = tempBt;
                    } else if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "") {
                        x = false;
                        y = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        for(y1 = 0; y1 < secondLength; ++y1) {
                            tempBt = enc(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                        }

                        encByte = tempBt;
                    } else if (firstKey != null && firstKey != "") {
                        x = false;
                        tempBt = tempByte;

                        for(x1 = 0; x1 < firstLength; ++x1) {
                            tempBt = enc(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                        }

                        encByte = tempBt;
                    }

                    encData = encData + bt64ToHex(encByte);
                }
            }
        }

        return encData;
    }

    public static String strDecode(String data, String... keys) {
        int key_length = null == keys ? 0 : keys.length;
        String firstKey = key_length > 0 ? keys[0] : null;
        String secondKey = key_length > 1 ? keys[1] : null;
        String thirdKey = key_length > 2 ? keys[2] : null;
        int leng = data.length();
        String decStr = "";
        List firstKeyBt = null;
        List secondKeyBt = null;
        List thirdKeyBt = null;
        int firstLength = 0;
        int secondLength = 0;
        int thirdLength = 0;
        if (firstKey != null && firstKey != "") {
            firstKeyBt = getKeyBytes(firstKey);
            firstLength = firstKeyBt.size();
        }

        if (secondKey != null && secondKey != "") {
            secondKeyBt = getKeyBytes(secondKey);
            secondLength = secondKeyBt.size();
        }

        if (thirdKey != null && thirdKey != "") {
            thirdKeyBt = getKeyBytes(thirdKey);
            thirdLength = thirdKeyBt.size();
        }

        int iterator = leng / 16;

        for(int i = 0; i < iterator; ++i) {
            String tempData = data.substring(i * 16 + 0, i * 16 + 16);
            String strByte = hexToBt64(tempData);
            int[] intByte = new int[64];

            for(int j = 0; j < 64; ++j) {
                intByte[j] = Integer.parseInt(strByte.substring(j, j + 1), 10);
            }

            int[] decByte = null;
            int[] tempBt;
            boolean x;
            boolean y;
            int x1;
            int y1;
            if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "" && thirdKey != null && thirdKey != "") {
                tempBt = intByte;

                for(x1 = thirdLength - 1; x1 >= 0; --x1) {
                    tempBt = dec(tempBt, (int[])((int[])thirdKeyBt.get(x1)));
                }

                for(y1 = secondLength - 1; y1 >= 0; --y1) {
                    tempBt = dec(tempBt, (int[])((int[])secondKeyBt.get(y1)));
                }

                for(int z = firstLength - 1; z >= 0; --z) {
                    tempBt = dec(tempBt, (int[])((int[])firstKeyBt.get(z)));
                }

                decByte = tempBt;
            } else if (firstKey != null && firstKey != "" && secondKey != null && secondKey != "") {
                x = false;
                y = false;
                tempBt = intByte;

                for(x1 = secondLength - 1; x1 >= 0; --x1) {
                    tempBt = dec(tempBt, (int[])((int[])secondKeyBt.get(x1)));
                }

                for(y1 = firstLength - 1; y1 >= 0; --y1) {
                    tempBt = dec(tempBt, (int[])((int[])firstKeyBt.get(y1)));
                }

                decByte = tempBt;
            } else if (firstKey != null && firstKey != "") {
                x = false;
                tempBt = intByte;

                for(x1 = firstLength - 1; x1 >= 0; --x1) {
                    tempBt = dec(tempBt, (int[])((int[])firstKeyBt.get(x1)));
                }

                decByte = tempBt;
            }

            decStr = decStr + byteToString(decByte);
        }

        return decStr;
    }

    private static List getKeyBytes(String key) {
        List keyBytes = new ArrayList();
        int leng = key.length();
        int iterator = leng / 4;
        int remainder = leng % 4;
        int i;
        for(i = 0; i < iterator; ++i) {
            keyBytes.add(i, strToBt(key.substring(i * 4 + 0, i * 4 + 4)));
        }

        if (remainder > 0) {
            keyBytes.add(i, strToBt(key.substring(i * 4 + 0, leng)));
        }

        return keyBytes;
    }

    private static int[] strToBt(String str) {
        int leng = str.length();
        int[] bt = new int[64];
        int i;
        int p;
        int q;
        if (leng < 4) {
            boolean i1 = false;
            boolean j1 = false;
            boolean p1 = false;
            boolean q1 = false;

            int pow;
            int m;
            boolean m1 = false;
            for(i = 0; i < leng; ++i) {
                int k = str.charAt(i);

                for(int j = 0; j < 16; ++j) {
                    pow = 1;
                    m1 = false;

                    for(m = 15; m > j; --m) {
                        pow *= 2;
                    }

                    bt[16 * i + j] = k / pow % 2;
                }
            }

            for(p = leng; p < 4; ++p) {
                int k = 0;

                for(q = 0; q < 16; ++q) {
                    pow = 1;
                    m1 = false;

                    for(m = 15; m > q; --m) {
                        pow *= 2;
                    }

                    bt[16 * p + q] = k / pow % 2;
                }
            }
        } else {
            for(i = 0; i < 4; ++i) {
                int k = str.charAt(i);

                for(p = 0; p < 16; ++p) {
                    q = 1;

                    for(int m = 15; m > p; --m) {
                        q *= 2;
                    }

                    bt[16 * i + p] = k / q % 2;
                }
            }
        }

        return bt;
    }

    private static String bt4ToHex(String binary) {
        String hex = "";
        if (binary.equalsIgnoreCase("0000")) {
            hex = "0";
        } else if (binary.equalsIgnoreCase("0001")) {
            hex = "1";
        } else if (binary.equalsIgnoreCase("0010")) {
            hex = "2";
        } else if (binary.equalsIgnoreCase("0011")) {
            hex = "3";
        } else if (binary.equalsIgnoreCase("0100")) {
            hex = "4";
        } else if (binary.equalsIgnoreCase("0101")) {
            hex = "5";
        } else if (binary.equalsIgnoreCase("0110")) {
            hex = "6";
        } else if (binary.equalsIgnoreCase("0111")) {
            hex = "7";
        } else if (binary.equalsIgnoreCase("1000")) {
            hex = "8";
        } else if (binary.equalsIgnoreCase("1001")) {
            hex = "9";
        } else if (binary.equalsIgnoreCase("1010")) {
            hex = "A";
        } else if (binary.equalsIgnoreCase("1011")) {
            hex = "B";
        } else if (binary.equalsIgnoreCase("1100")) {
            hex = "C";
        } else if (binary.equalsIgnoreCase("1101")) {
            hex = "D";
        } else if (binary.equalsIgnoreCase("1110")) {
            hex = "E";
        } else if (binary.equalsIgnoreCase("1111")) {
            hex = "F";
        }

        return hex;
    }

    private static String hexToBt4(String hex) {
        String binary = "";
        if (hex.equalsIgnoreCase("0")) {
            binary = "0000";
        } else if (hex.equalsIgnoreCase("1")) {
            binary = "0001";
        }

        if (hex.equalsIgnoreCase("2")) {
            binary = "0010";
        }

        if (hex.equalsIgnoreCase("3")) {
            binary = "0011";
        }

        if (hex.equalsIgnoreCase("4")) {
            binary = "0100";
        }

        if (hex.equalsIgnoreCase("5")) {
            binary = "0101";
        }

        if (hex.equalsIgnoreCase("6")) {
            binary = "0110";
        }

        if (hex.equalsIgnoreCase("7")) {
            binary = "0111";
        }

        if (hex.equalsIgnoreCase("8")) {
            binary = "1000";
        }

        if (hex.equalsIgnoreCase("9")) {
            binary = "1001";
        }

        if (hex.equalsIgnoreCase("A")) {
            binary = "1010";
        }

        if (hex.equalsIgnoreCase("B")) {
            binary = "1011";
        }

        if (hex.equalsIgnoreCase("C")) {
            binary = "1100";
        }

        if (hex.equalsIgnoreCase("D")) {
            binary = "1101";
        }

        if (hex.equalsIgnoreCase("E")) {
            binary = "1110";
        }

        if (hex.equalsIgnoreCase("F")) {
            binary = "1111";
        }

        return binary;
    }

    private static String byteToString(int[] byteData) {
        String str = "";

        for(int i = 0; i < 4; ++i) {
            int count = 0;

            for(int j = 0; j < 16; ++j) {
                int pow = 1;

                for(int m = 15; m > j; --m) {
                    pow *= 2;
                }

                count += byteData[16 * i + j] * pow;
            }

            if (count != 0) {
                str = str + "" + (char)count;
            }
        }

        return str;
    }

    private static String bt64ToHex(int[] byteData) {
        String hex = "";

        for(int i = 0; i < 16; ++i) {
            String bt = "";

            for(int j = 0; j < 4; ++j) {
                bt = bt + byteData[i * 4 + j];
            }

            hex = hex + bt4ToHex(bt);
        }

        return hex;
    }

    private static String hexToBt64(String hex) {
        String binary = "";

        for(int i = 0; i < 16; ++i) {
            binary = binary + hexToBt4(hex.substring(i, i + 1));
        }

        return binary;
    }

    private static int[] enc(int[] dataByte, int[] keyByte) {
        int[][] keys = generateKeys(keyByte);
        int[] ipByte = initPermute(dataByte);
        int[] ipLeft = new int[32];
        int[] ipRight = new int[32];
        int[] tempLeft = new int[32];
        boolean i1 = false;
        boolean j1 = false;
        boolean k1 = false;
        boolean m1 = false;
        boolean n1 = false;

        for(int k = 0; k < 32; ++k) {
            ipLeft[k] = ipByte[k];
            ipRight[k] = ipByte[32 + k];
        }

        int[] key;
        int i;
        for(i = 0; i < 16; ++i) {
            for(int j = 0; j < 32; ++j) {
                tempLeft[j] = ipLeft[j];
                ipLeft[j] = ipRight[j];
            }

            key = new int[48];

            for(int m = 0; m < 48; ++m) {
                key[m] = keys[i][m];
            }

            int[] tempRight = xor(pPermute(sBoxPermute(xor(expandPermute(ipRight), key))), tempLeft);

            for(int n = 0; n < 32; ++n) {
                ipRight[n] = tempRight[n];
            }
        }

        key = new int[64];

        for(i = 0; i < 32; ++i) {
            key[i] = ipRight[i];
            key[32 + i] = ipLeft[i];
        }

        return finallyPermute(key);
    }

    private static int[] dec(int[] dataByte, int[] keyByte) {
        int[][] keys = generateKeys(keyByte);
        int[] ipByte = initPermute(dataByte);
        int[] ipLeft = new int[32];
        int[] ipRight = new int[32];
        int[] tempLeft = new int[32];
        boolean i1 = false;
        boolean j1 = false;
        boolean k1 = false;
        boolean m1 = false;
        boolean n1 = false;

        for(int k = 0; k < 32; ++k) {
            ipLeft[k] = ipByte[k];
            ipRight[k] = ipByte[32 + k];
        }

        int[] key;
        int i;
        for(i = 15; i >= 0; --i) {
            for(int j = 0; j < 32; ++j) {
                tempLeft[j] = ipLeft[j];
                ipLeft[j] = ipRight[j];
            }

            key = new int[48];

            for(int m = 0; m < 48; ++m) {
                key[m] = keys[i][m];
            }

            int[] tempRight = xor(pPermute(sBoxPermute(xor(expandPermute(ipRight), key))), tempLeft);

            for(int n = 0; n < 32; ++n) {
                ipRight[n] = tempRight[n];
            }
        }

        key = new int[64];

        for(i = 0; i < 32; ++i) {
            key[i] = ipRight[i];
            key[32 + i] = ipLeft[i];
        }

        return finallyPermute(key);
    }

    private static int[] initPermute(int[] originalData) {
        int[] ipByte = new int[64];
        boolean i1 = false;
        boolean m1 = true;
        boolean n1 = false;
        int i = 0;
        int m = 1;

        for(int n = 0; i < 4; n += 2) {
            int j = 7;

            for(int k = 0; j >= 0; ++k) {
                ipByte[i * 8 + k] = originalData[j * 8 + m];
                ipByte[i * 8 + k + 32] = originalData[j * 8 + n];
                --j;
            }

            ++i;
            m += 2;
        }

        return ipByte;
    }

    private static int[] expandPermute(int[] rightData) {
        int[] epByte = new int[48];
        for(int i = 0; i < 8; ++i) {
            if (i == 0) {
                epByte[i * 6 + 0] = rightData[31];
            } else {
                epByte[i * 6 + 0] = rightData[i * 4 - 1];
            }

            epByte[i * 6 + 1] = rightData[i * 4 + 0];
            epByte[i * 6 + 2] = rightData[i * 4 + 1];
            epByte[i * 6 + 3] = rightData[i * 4 + 2];
            epByte[i * 6 + 4] = rightData[i * 4 + 3];
            if (i == 7) {
                epByte[i * 6 + 5] = rightData[0];
            } else {
                epByte[i * 6 + 5] = rightData[i * 4 + 4];
            }
        }

        return epByte;
    }

    private static int[] xor(int[] byteOne, int[] byteTwo) {
        int[] xorByte = new int[byteOne.length];

        for(int i = 0; i < byteOne.length; ++i) {
            xorByte[i] = byteOne[i] ^ byteTwo[i];
        }

        return xorByte;
    }

    private static int[] sBoxPermute(int[] expandByte) {
        int[] sBoxByte = new int[32];
        String binary = "";
        int[][] s1 = new int[][]{{14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7}, {0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8}, {4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0}, {15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13}};
        int[][] s2 = new int[][]{{15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10}, {3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5}, {0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15}, {13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9}};
        int[][] s3 = new int[][]{{10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8}, {13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1}, {13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7}, {1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12}};
        int[][] s4 = new int[][]{{7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15}, {13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9}, {10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4}, {3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14}};
        int[][] s5 = new int[][]{{2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9}, {14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6}, {4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14}, {11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3}};
        int[][] s6 = new int[][]{{12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11}, {10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8}, {9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6}, {4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13}};
        int[][] s7 = new int[][]{{4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1}, {13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6}, {1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2}, {6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12}};
        int[][] s8 = new int[][]{{13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7}, {1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2}, {7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8}, {2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11}};

        for(int m = 0; m < 8; ++m) {
            int i = expandByte[m * 6 + 0] * 2 + expandByte[m * 6 + 5];
            int j = expandByte[m * 6 + 1] * 2 * 2 * 2 + expandByte[m * 6 + 2] * 2 * 2 + expandByte[m * 6 + 3] * 2 + expandByte[m * 6 + 4];
            switch(m) {
                case 0:
                    binary = getBoxBinary(s1[i][j]);
                    break;
                case 1:
                    binary = getBoxBinary(s2[i][j]);
                    break;
                case 2:
                    binary = getBoxBinary(s3[i][j]);
                    break;
                case 3:
                    binary = getBoxBinary(s4[i][j]);
                    break;
                case 4:
                    binary = getBoxBinary(s5[i][j]);
                    break;
                case 5:
                    binary = getBoxBinary(s6[i][j]);
                    break;
                case 6:
                    binary = getBoxBinary(s7[i][j]);
                    break;
                case 7:
                    binary = getBoxBinary(s8[i][j]);
            }

            sBoxByte[m * 4 + 0] = Integer.parseInt(binary.substring(0, 1));
            sBoxByte[m * 4 + 1] = Integer.parseInt(binary.substring(1, 2));
            sBoxByte[m * 4 + 2] = Integer.parseInt(binary.substring(2, 3));
            sBoxByte[m * 4 + 3] = Integer.parseInt(binary.substring(3, 4));
        }

        return sBoxByte;
    }

    private static int[] pPermute(int[] sBoxByte) {
        int[] pBoxPermute = new int[]{sBoxByte[15], sBoxByte[6], sBoxByte[19], sBoxByte[20], sBoxByte[28], sBoxByte[11], sBoxByte[27], sBoxByte[16], sBoxByte[0], sBoxByte[14], sBoxByte[22], sBoxByte[25], sBoxByte[4], sBoxByte[17], sBoxByte[30], sBoxByte[9], sBoxByte[1], sBoxByte[7], sBoxByte[23], sBoxByte[13], sBoxByte[31], sBoxByte[26], sBoxByte[2], sBoxByte[8], sBoxByte[18], sBoxByte[12], sBoxByte[29], sBoxByte[5], sBoxByte[21], sBoxByte[10], sBoxByte[3], sBoxByte[24]};
        return pBoxPermute;
    }

    private static int[] finallyPermute(int[] endByte) {
        int[] fpByte = new int[]{endByte[39], endByte[7], endByte[47], endByte[15], endByte[55], endByte[23], endByte[63], endByte[31], endByte[38], endByte[6], endByte[46], endByte[14], endByte[54], endByte[22], endByte[62], endByte[30], endByte[37], endByte[5], endByte[45], endByte[13], endByte[53], endByte[21], endByte[61], endByte[29], endByte[36], endByte[4], endByte[44], endByte[12], endByte[52], endByte[20], endByte[60], endByte[28], endByte[35], endByte[3], endByte[43], endByte[11], endByte[51], endByte[19], endByte[59], endByte[27], endByte[34], endByte[2], endByte[42], endByte[10], endByte[50], endByte[18], endByte[58], endByte[26], endByte[33], endByte[1], endByte[41], endByte[9], endByte[49], endByte[17], endByte[57], endByte[25], endByte[32], endByte[0], endByte[40], endByte[8], endByte[48], endByte[16], endByte[56], endByte[24]};
        return fpByte;
    }

    public static String getBoxBinary(int i) {
        String binary = "";
        switch(i) {
            case 0:
                binary = "0000";
                break;
            case 1:
                binary = "0001";
                break;
            case 2:
                binary = "0010";
                break;
            case 3:
                binary = "0011";
                break;
            case 4:
                binary = "0100";
                break;
            case 5:
                binary = "0101";
                break;
            case 6:
                binary = "0110";
                break;
            case 7:
                binary = "0111";
                break;
            case 8:
                binary = "1000";
                break;
            case 9:
                binary = "1001";
                break;
            case 10:
                binary = "1010";
                break;
            case 11:
                binary = "1011";
                break;
            case 12:
                binary = "1100";
                break;
            case 13:
                binary = "1101";
                break;
            case 14:
                binary = "1110";
                break;
            case 15:
                binary = "1111";
        }

        return binary;
    }

    private static int[][] generateKeys(int[] keyByte) {
        int[][] keys = new int[16][48];
        int[] key = new int[56];
        int[] loop = new int[]{1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1};

        int i;
        int tempLeft;
        int tempRight;
        for(i = 0; i < 7; ++i) {
            tempLeft = 0;

            for(tempRight = 7; tempLeft < 8; --tempRight) {
                key[i * 8 + tempLeft] = keyByte[8 * tempRight + i];
                ++tempLeft;
            }
        }

        label152:
        for(i = 0; i < 16; ++i) {
            boolean tempLeft1 = false;
            boolean tempRight1 = false;

            int m;
            for(int j = 0; j < loop[i]; ++j) {
                tempLeft = key[0];
                tempRight = key[28];

                for(m = 0; m < 27; ++m) {
                    key[m] = key[m + 1];
                    key[28 + m] = key[29 + m];
                }

                key[27] = tempLeft;
                key[55] = tempRight;
            }

            int[] tempKey = new int[]{key[13], key[16], key[10], key[23], key[0], key[4], key[2], key[27], key[14], key[5], key[20], key[9], key[22], key[18], key[11], key[3], key[25], key[7], key[15], key[6], key[26], key[19], key[12], key[1], key[40], key[51], key[30], key[36], key[46], key[54], key[29], key[39], key[50], key[44], key[32], key[47], key[43], key[48], key[38], key[55], key[33], key[52], key[45], key[41], key[49], key[35], key[28], key[31]};
            switch(i) {
                case 0:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[0][m] = tempKey[m];
                        ++m;
                    }
                case 1:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[1][m] = tempKey[m];
                        ++m;
                    }
                case 2:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[2][m] = tempKey[m];
                        ++m;
                    }
                case 3:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[3][m] = tempKey[m];
                        ++m;
                    }
                case 4:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[4][m] = tempKey[m];
                        ++m;
                    }
                case 5:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[5][m] = tempKey[m];
                        ++m;
                    }
                case 6:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[6][m] = tempKey[m];
                        ++m;
                    }
                case 7:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[7][m] = tempKey[m];
                        ++m;
                    }
                case 8:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[8][m] = tempKey[m];
                        ++m;
                    }
                case 9:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[9][m] = tempKey[m];
                        ++m;
                    }
                case 10:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[10][m] = tempKey[m];
                        ++m;
                    }
                case 11:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[11][m] = tempKey[m];
                        ++m;
                    }
                case 12:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[12][m] = tempKey[m];
                        ++m;
                    }
                case 13:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[13][m] = tempKey[m];
                        ++m;
                    }
                case 14:
                    m = 0;

                    while(true) {
                        if (m >= 48) {
                            continue label152;
                        }

                        keys[14][m] = tempKey[m];
                        ++m;
                    }
                case 15:
                    for(m = 0; m < 48; ++m) {
                        keys[15][m] = tempKey[m];
                    }
            }
        }

        return keys;
    }
}
