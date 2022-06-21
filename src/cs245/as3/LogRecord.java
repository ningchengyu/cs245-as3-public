package cs245.as3;

//日志类型
class Type {

    public final static int WRITE = 0;//一个操作
    public final static int COMMIT = 1;//提交

}

//日志记录
/*
每条日记记录拥有的字段为：事务编号txID，类型type，日志大小size，和一对键值对key、value。
其中type只需关注write操作和commit操作，分别对应0和1，其中write类型的日志键值对不为空，commit类型的日志键值对为空。
这样可以通过查询type值来判断日志的类型。
类中包含的方法是所有属性的getset方法，其中在写一条日志的时候一般不显式地赋值大小。
因为日志的大小与日志类型是有关的，所以大小在写入类型和写入value的时候获取。
如果是commit 类型，则size为固定的3个字段的长度，为16；
如果是write，则在赋值value的时候将size设置为前4个字段的长度（24）加value的长度。
 */
public class LogRecord {

    private long txID;
    private int type;
    private int size;
    private long key;
    private byte[] value;


    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
        if (type == Type.COMMIT) {
            this.size = 16;
        }
    }

    public long getTxID() {
        return txID;
    }

    public void setTxID(long txID) {
        this.txID = txID;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
        if (type == Type.WRITE) {
            this.size = 24 + value.length;
        }
    }

}


