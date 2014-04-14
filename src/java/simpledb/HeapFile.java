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
    //@ADDED
    public File file = null;
    public TupleDesc td = null;
    //@ADDED

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) { //@ADDED
        //System.out.println(f.getAbsolutePath());

        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() { //@ADDED
        return file;
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
    public int getId() { //@ADDED
        return file.getAbsoluteFile().hashCode(); //@TODO: hash?
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() { //@ADDED
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException { //@ADDED
        byte[] data = null;
        Page pg = null;
        RandomAccessFile stream = null;

        //System.out.println("pageNumber=" + pid.pageNumber() + 
                           //"\ttableName=" + Database.getCatalog().getTableName(pid.getTableId()) + 
                           //"\tnumPages=" + numPages());

        try{
            stream = new RandomAccessFile(file,"r");
            if(pid.pageNumber() >= numPages()){
                throw new IllegalArgumentException();
            }
            data = new byte[BufferPool.PAGE_SIZE];
            stream.seek(pid.pageNumber()*BufferPool.PAGE_SIZE);
            stream.read(data);
            stream.close();

            pg = new HeapPage(new HeapPageId(pid.getTableId(),pid.pageNumber()),data);

        }catch(FileNotFoundException e){
            System.out.println("File not found! Full path:" + file.getAbsolutePath());
            assert false : "File:" + file + "\nThis should not happen!";
        }catch(IOException e){
            System.out.println("IO Exception:" + e);
            assert false : "Pid:" + pid + "\nThis should not happen!";
        } finally{
            assert pg != null;
            return pg;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException { //@ADDED
        int offset = page.getId().pageNumber();
        byte[] data = page.getPageData();

        RandomAccessFile rfile = new RandomAccessFile(file,"rw");
        rfile.seek(offset * BufferPool.PAGE_SIZE);
        rfile.write(data);
        rfile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() { //@ADDED
        int n = (int)(file.length() / (long)BufferPool.PAGE_SIZE);
        assert (n*BufferPool.PAGE_SIZE) == file.length();

        return n;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException { //@ADDED

        if(!t.getTupleDesc().equals(td)){
            throw new DbException("tupleDesc mismatch");
        }

        BufferPool pool = Database.getBufferPool();
        ArrayList<Page> page_mod = new ArrayList<Page>();
        boolean success = false;
        int L = numPages();
        int index = -1;
        HeapPage pg = null;

        for(int i=0;i<L;i++){
            PageId pid = new HeapPageId(getId(),i);
            pg = (HeapPage)pool.getPage(tid,pid,Permissions.READ_WRITE);

            try{
                pg.insertTuple(t);
                success = true;
            } catch (DbException e){ success = false; }
            
            if(success){
                pg.markDirty(true,tid);
                page_mod.add(pg);

                index = i;

                break;
            }
        }
        if(success){
            //System.out.println("adding to page #" + index);
            //System.out.println("empty slots: " + pg.getNumEmptySlots());
            return page_mod;
        }

        //appending new page
        HeapPageId pid = new HeapPageId(getId(),L);
        pg = new HeapPage(pid,HeapPage.createEmptyPageData());
        pg.insertTuple(t);

        RandomAccessFile rfile = new RandomAccessFile(file,"rw");
        rfile.setLength(rfile.length() + BufferPool.PAGE_SIZE);
        rfile.close();

        writePage(pg);
        assert numPages() == L+1;

        page_mod.add(pg);

        return page_mod;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException { //@ADDED
        PageId pid = t.getRecordId().getPageId();

        if(pid.getTableId() != getId()){
            throw new DbException("tuple not on this table");
        }
        if(pid.pageNumber() < 0 || pid.pageNumber() >= numPages()){
            throw new DbException("page number out of bound");
        }

        BufferPool pool = Database.getBufferPool();
        HeapPage pg = (HeapPage)pool.getPage(tid,pid,Permissions.READ_WRITE);
        pg.deleteTuple(t);
        pg.markDirty(true,tid);

        return pg;
    }

    public interface DbFileIteratorPage extends DbFileIterator{
        public boolean pageIterHasNext() throws DbException, TransactionAbortedException;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) { //@ADDED
        final TransactionId f_tid = tid;

        return new DbFileIteratorPage() { //@ADDED
            public boolean opened = false;
            public boolean closed = false;
            public Iterator<Tuple> current_iter = null;
            public int pageIndex = -1;

            private Iterator<Tuple> getTupleIter() throws TransactionAbortedException,DbException{
                assert opened;

                BufferPool pool = Database.getBufferPool();
                HeapPageId pid = new HeapPageId(HeapFile.this.getId(),pageIndex);
                HeapPage page = (HeapPage)pool.getPage(f_tid,pid,Permissions.READ_WRITE);

                return page.iterator();
            }

            public void open() throws DbException, TransactionAbortedException {
                if(opened) return;

                opened = true;
                pageIndex = 0;
                current_iter = getTupleIter();
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!opened) return false; // throw new DbException("iterator not opened");
                if(closed) throw new DbException("iterator closed!");

                while(!current_iter.hasNext()){
                    if(pageIndex >= HeapFile.this.numPages()-1)
                        return false;

                    pageIndex++;
                    current_iter = getTupleIter();
                    assert current_iter != null;
                }

                return true;
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
                if(!opened) throw new NoSuchElementException(); //DbException("iterator not opened");
                if(closed) throw new DbException("iterator closed!");

                if(!hasNext())
                    throw new NoSuchElementException();

                return current_iter.next();
            }

            public void rewind() throws DbException, TransactionAbortedException{
                if(!opened) throw new DbException("iterator not opened");
                if(closed) throw new DbException("iterator closed!");

                pageIndex = 0;
                current_iter = getTupleIter();
            }

            public void close(){
                if(closed) return;

                current_iter = null;
                pageIndex = -1;

                closed = true;
                opened = false;
            }

            public boolean pageIterHasNext() throws DbException, TransactionAbortedException{
                if(!opened) throw new NoSuchElementException(); //DbException("iterator not opened");
                if(closed) throw new DbException("iterator closed!");

                return current_iter.hasNext();
            }

        };
    }

}

