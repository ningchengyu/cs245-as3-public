package cs245.as3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/*
本类包含将日志记录序列化的方法，以及重做时将序列化字节解析成日志记录的反序列化方法
其中，序列化方法将事务ID、日志类型与日志记录大小写进去，
然后判断类型，如果是write类型，则还要把key和value写进去
反序列化方法则反过来，先获取事务ID、日志类型与日志记录大小，
然后判断类型，如果是write类型，则还要获取key和value
 */
public class Serialize {

    public static byte[] serialize(LogRecord logRecord) {
        try{
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream oos = new DataOutputStream(bos);
            oos.writeLong(logRecord.getTxID());
            oos.writeInt(logRecord.getType());
            oos.writeInt(logRecord.getSize());
            if(logRecord.getType() == Type.WRITE){
                oos.writeLong(logRecord.getKey());
                for(int i = 0; i< logRecord.getValue().length; i++){
                    oos.writeByte(logRecord.getValue()[i]);
                }
            }
            oos.flush();
            byte[] records = bos.toByteArray();
            oos.close();
            bos.close();
            return records;
        }catch(IOException e){
            return null;
        }
    }

    public static LogRecord deserialize(byte[] recordbytes) {
        ByteBuffer buff = ByteBuffer.wrap(recordbytes);
        long txID=buff.getLong();
        int type = buff.getInt();
        int size = buff.getInt();
        LogRecord logRecord=new LogRecord();
        logRecord.setType(type);
        logRecord.setTxID(txID);
        logRecord.setSize(size);
        if(logRecord.getType() ==Type.WRITE){
            Long key = buff.getLong();
            logRecord.setKey(key);
            int valuesize=logRecord.getSize()-24;
            logRecord.setValue(new byte[valuesize]);
            for(int i=0;i<valuesize;i++){
                logRecord.getValue()[i]=buff.get();
            }
        }
        return logRecord;
    }

}
