package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	static class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	/*
	为每一个key维护一个最终的value值
	在commit提交时将key和value写进去，initAndRecover时从sm中获取值，供read读取事务的最新提交值
	 */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	/*
	每个事务提交前，将key、value序列记录在writesets中
	在write中为事务添加新的key\value值，
	并在commit提交时取出每个事务的key\value序列并写入日志中，
	提交成功后就可以将移除writesets中的对应事务了。
	注意，进行abort操作时也需要移除writesets中对应的事务
	 */
	private final HashMap<Long, ArrayList<WritesetEntry>> writesets;

	/*
	记录每个事务的日志列表。
	在开始一个事务时，在logRecordSets中新增这个事务和对应的空白日志列表
	进行write操作时，则生成类型为write的日志记录，添加到对应事务的日志列表中。
	直到commit时，生成类型为commit的日志记录，添加到对应事务的日志列表中,
	然后就可以把这个事务的所有日志一条一条地序列化，写入lm的log中
	与writeset一样，进行abort操作时需要移除logRecordSets中对应的事务
	 */
	private final HashMap<Long,ArrayList<LogRecord>> logRecordSets;

	private StorageManager sm;
	private LogManager lm;

	/**
	 * 日志偏移量 tag 唯一对应的 key,维护日志偏移量的最小值,用于在数据持久化过程中合理设置日志截断点.
	 * 其内容在commit时添加，initAndRecover时也需要添加，
	 * 直到写持久化时删除对应项。
	 * keyWithTag也一样
	 * */
	private final TreeMap<Long,Long> tagWithKey;

	/**
	 * key 映射到 key 的最新值的日志偏移量 tag.
	 * */
	private final HashMap<Long,Long> keyWithTag;

	public TransactionManager() {
		writesets = new HashMap<>();
		latestValues = null;
		logRecordSets = new HashMap<>();
		tagWithKey = new TreeMap<>();
		keyWithTag = new HashMap<>();
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	/*
	进行初始化及恢复，包括读取日志文件及持久化
	具体地，在从lm中读取日志后进行反序列化，记录已提交的日志，对已提交的日志进行重做，并持久化写入。
	在这个过程中，更新latestValues、tagWithKey和keyWithTag
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {

		this.sm = sm;
		this.lm = lm;
		latestValues = sm.readStoredTable();

		/*
		 * 读取所有日志文件
		 */
		ArrayList<LogRecord> logRecords=new ArrayList<>();
		ArrayList<Integer> logtags=new ArrayList<>();
		HashSet<Long> txCommit=new HashSet<>();
		for(int offset = lm.getLogTruncationOffset(); offset< lm.getLogEndOffset();){//读取日志

			byte[] bytes= lm.readLogRecord(offset, 16);
			int size = (((bytes[12] & 0xff) << 24) |
					((bytes[12 + 1] & 0xff) << 16) |
					((bytes[12 + 2] & 0xff) << 8) |
					(bytes[12 + 3] & 0xff));
			ByteBuffer buff=ByteBuffer.allocate(size);
			for(int i=0;i<size;i+=128){
				int l=Math.min(size-i,128);
				buff.put(lm.readLogRecord(offset+i, l));
			}
			byte[] recordbytes=buff.array();
			LogRecord logRecord=Serialize.deserialize(recordbytes);//反序列化

			logRecords.add(logRecord);
			if(logRecord.getType()==Type.COMMIT){
				//记录提交日志
				txCommit.add(logRecord.getTxID());
			}
			offset+=logRecord.getSize();
			logtags.add(offset);
		}
		/*
		 * 持久化：对已经提交的事务的日志重做,并持久化写入
		 */

		Iterator<Integer> it=logtags.iterator();
		for(LogRecord rd:logRecords){
			if(txCommit.contains(rd.getTxID()) && rd.getType()==Type.WRITE){
				long tag = it.next();
				latestValues.put(rd.getKey(), new TaggedValue(tag, rd.getValue()));
				keyWithTag.put(rd.getKey(),tag);
				tagWithKey.put(tag,rd.getKey());
				sm.queueWrite(rd.getKey(), tag, rd.getValue());
			}
		}


	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	/*
	新事务开始，在logRecordSets中新增项，记录事务ID，并生成一个空日志列表
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		ArrayList<LogRecord> logRecords = new ArrayList<>();
		logRecordSets.put(txID, logRecords);
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	/*
    利用latestValues读取事务的最新提交值
	 */
	public byte[] read(long txID, long key) {
 		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read()
	 * calls until the transaction making the write commits. For simplicity, we will not make reads
	 * to this same key from txID itself after we make a write to the key.
	 */
	/*
	写操作时，生成写日志并放在logRecordSets对应事务的日志列表中
	并将key value值记入writesets对应事务的列表中
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		LogRecord logRecord = new LogRecord();
		logRecord.setTxID(txID);
		logRecord.setType(Type.WRITE);
		logRecord.setKey(key);
		logRecord.setValue(value);

		logRecordSets.get(txID).add(logRecord);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}

	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	/*
	提交操作，
	生成提交日志，放在logRecordSets对应事务的日志列表中
	将logRecordSets中该事务的日志一条一条序列化，写入lm的log
	并将writesets中该事务的键值写入sm的entries中
	同时更新latestValues，tagWithKey和keyWithTag变量
	 */
	public void commit(long txID) {
		LogRecord logRecord = new LogRecord();
		logRecord.setTxID(txID);
		logRecord.setType(Type.COMMIT);
		logRecordSets.get(txID).add(logRecord);

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		HashMap<Long,Long> keyTag= new HashMap<>();
		for(LogRecord rd: logRecordSets.get(txID)) {
			long key = rd.getKey();
			long tag = 0;
			byte[] bytes = Serialize.serialize(rd);//序列化
			int size = rd.getSize();
			assert bytes != null;
			ByteBuffer buff = ByteBuffer.wrap(bytes);
			for (int i = 0; i < size; i = i + 128) {
				int l = Math.min(size - i, 128);
				byte[] tmp = new byte[l];
				buff.get(tmp, i, l);
				tag = lm.appendLogRecord(tmp);
			}
			if (rd.getType() == Type.WRITE) {
				keyTag.put(key, tag);
			}

		}
		if (writeset != null) {
			for (WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tagg = keyTag.get(x.key);
				latestValues.put(x.key, new TaggedValue(tagg, x.value));
				sm.queueWrite(x.key, tagg, x.value);

				// 检查更新 key 对应的 日志偏移量 tag,同时也检查它的辅助哈希表.
				if (keyWithTag.containsKey(x.key)) {
					long tag = keyWithTag.get(x.key);
					tagWithKey.remove(tag);
				}
				keyWithTag.put(x.key,keyTag.get(x.key));
				tagWithKey.put(keyTag.get(x.key),x.key);
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	/*
	回撤操作，删除writesets和logRecordSets中的对应事务
	 */
	public void abort(long txID) {
		writesets.remove(txID);
		logRecordSets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */

	/*
	写持久化，更新tagWithKey和keyWithTag，删除当前已经持久化的 key.
	并根据tagWithKey将日志截断到事务成功日志偏移量的最小值处.
	 */
	public void writePersisted(long persisted_key, long persisted_tag, byte[] persisted_value) {
		// 删除当前已经持久化的 key.
		tagWithKey.remove(persisted_tag);
		keyWithTag.remove(persisted_key);
		if (!tagWithKey.isEmpty()) {
			long tag = tagWithKey.firstKey();
			// 日志截断到事务成功日志偏移量的最小值处.
			lm.setLogTruncationOffset((int)tag);
		}
	}

}
