package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File f;
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        // return null;
    	return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
    	return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
    	return td;
    }

    // see DbFile.java for javadocs
    // @@exception should be optimize@@
    public Page readPage(PageId pid) {
        // some code goes here
        // return null;
    	// return Database.getBufferPool().
    	// return null;
    	int pageNo = pid.pageNumber();
    	try {
			RandomAccessFile dis = new RandomAccessFile(f, "r");
			int pageSize = BufferPool.PAGE_SIZE;
	    	int offset = pageNo * pageSize;
	    	byte[] pageContent = new byte[pageSize];
	    	dis.seek(offset);
			dis.read(pageContent, 0, pageSize);
			dis.close();
			return new HeapPage((HeapPageId)pid, pageContent);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // return 0;
    	BufferPool bp = Database.getBufferPool();
    	int pageSize = bp.PAGE_SIZE;
    	long fileSize = f.length();
    	return (int)((fileSize) / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }
    
    
    class HeapFileIterator implements DbFileIterator {
		private static final long serialVersionUID = 1L;
		private Iterator<Tuple> tupleIterator;
    	private int pgNo;
    	private TransactionId tid;
    	public HeapFileIterator(TransactionId tid) {
    		this.tid = tid;
    		tupleIterator = null;
    		pgNo = 0;
    	}
    	public void open() throws TransactionAbortedException, DbException {
    		HeapPageId hpi = new HeapPageId(getId(), pgNo);
    		BufferPool bp = Database.getBufferPool();
    		HeapPage hp = (HeapPage)bp.getPage(tid, hpi, null);
    		tupleIterator = hp.iterator();
    	}
    	public boolean hasNext() throws TransactionAbortedException, DbException {
    		if(tupleIterator == null) return false;
    		if(tupleIterator.hasNext()) return true;
    		int numPages = numPages();
    		while(true) {
    			pgNo++;
    			if(pgNo >= numPages) return false;
    			HeapPageId hpi = new HeapPageId(getId(), pgNo);
    			BufferPool bp = Database.getBufferPool();
    			HeapPage hp = (HeapPage)bp.getPage(tid, hpi, null);
    			tupleIterator = hp.iterator();
    			if(tupleIterator.hasNext()) return true;
    		}
    	}
    	public Tuple next() {
    		if(tupleIterator == null) throw new NoSuchElementException();
    		return tupleIterator.next();
    	}
    	public void rewind() throws TransactionAbortedException, DbException {
    		open();
    	}
    	public void close() {
    		this.pgNo = 0;
    		this.tupleIterator = null;
    	}
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        // return null;
    	// return null;
    	return new HeapFileIterator(tid);
    	
    }

}

