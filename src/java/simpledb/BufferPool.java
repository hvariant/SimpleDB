package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;
    public static int atime = 0;

    public class PageEntry{
        public Page pg = null;
        public int pg_atime;

        public PageEntry(Page pg){
            this.pg = pg;
            this.pg_atime = BufferPool.atime++;
        }
        
        public void visit(){
            this.pg_atime = BufferPool.atime++;
        }
    }

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public int numPages = 0;
    public Map<PageId,PageEntry> id_page = null;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) { //@ADDED
        id_page = new HashMap<PageId,PageEntry>();
        this.numPages = numPages;
        
        //System.out.println("pages="+numPages);
        //for(StackTraceElement tse : (new Throwable()).getStackTrace()){
            //System.out.println(tse);
        //}
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        PageEntry pg_e = id_page.get(pid);
        if(pg_e != null){
            pg_e.visit();
            return pg_e.pg;
        }

        while(id_page.size() >= numPages){
            evictPage();
        }

        PageId id = new HeapPageId(pid.getTableId(),pid.pageNumber());
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());

        Page pg = file.readPage(id);
        pg_e = new PageEntry(pg);
        id_page.put(id,pg_e);

        return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException { //@ADDED
        DbFile file = Database.getCatalog().getDbFile(tableId);
        file.insertTuple(tid,t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException { //@ADDED
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDbFile(tableId);
        file.deleteTuple(tid,t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException { //@ADDED
        Iterator iter = id_page.keySet().iterator();

        while(iter.hasNext()){
            PageId pid = (PageId)iter.next();
            Page pg = ((PageEntry)id_page.get(pid)).pg;

            if(pg.isDirty() != null)
                flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) { //@ADDED
        id_page.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException { //@ADDED
        Page pg = ((PageEntry)id_page.get(pid)).pg;
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        file.writePage(pg);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException { //@ADDED
        Iterator iter = id_page.keySet().iterator();

        while(iter.hasNext()){
            PageId pid = (PageId)iter.next();
            Page pg = ((PageEntry)id_page.get(pid)).pg;
            flushPage(pid);

            if(tid.equals(pg.isDirty())){
                flushPage(pid);
                pg.markDirty(false,tid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException { //@TODO
        Iterator<Map.Entry<PageId,PageEntry>> iter = id_page.entrySet().iterator();

        int oldest_atime = -1;
        PageId oldest_pid = null;

        while(iter.hasNext()){
            Map.Entry<PageId,PageEntry> entry = iter.next();
            PageId key = entry.getKey();
            PageEntry value = entry.getValue();
            if(oldest_pid == null || value.pg_atime < oldest_atime){
                oldest_atime = value.pg_atime;
                oldest_pid = key;
            }
        }

        assert oldest_pid != null;
        try{
            flushPage(oldest_pid);
        } catch (IOException e){
            throw new DbException("error when flushing page:" + e);
        }

        discardPage(oldest_pid);
    }
}
