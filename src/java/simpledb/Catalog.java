package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
    //@ADDED
    public Map<Integer,DbFile> id_file = null;
    public Map<Integer,String> id_name = null;
    public Map<Integer,String> id_pkey = null;
    public Map<String,DbFile> name_file = null;
    //@ADDED

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() { //@ADDED
        id_file = new HashMap<Integer,DbFile>();
        id_name = new HashMap<Integer,String>();
        id_pkey = new HashMap<Integer,String>();
        name_file = new HashMap<String,DbFile>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) { //@ADDED
        //System.out.println("adding table:" + name);

        assert name != null;
        assert pkeyField != null;
        assert file != null;

        DbFile prev = name_file.get(name);
        if(prev != null){
            id_file.remove(prev.getId());
            id_name.remove(prev.getId());
            id_pkey.remove(prev.getId());
        }

        id_file.put(file.getId(),file);
        id_name.put(file.getId(),name);
        id_pkey.put(file.getId(),pkeyField);
        name_file.put(name,file);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException { //@ADDED
        DbFile file = name_file.get(name);
        if(file == null)
            throw new NoSuchElementException();

        return file.getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException { //@ADDED
        DbFile file = getDbFile(tableid);
        
        return file.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException { //@ADDED
        DbFile file = id_file.get(tableid);

        if(file == null)
            throw new NoSuchElementException();

        return file;
    }

    public String getPrimaryKey(int tableid) { //@ADDED
        String pkey = id_pkey.get(tableid);

        if(pkey == null)
            throw new NoSuchElementException();

        return pkey;
    }

    public Iterator<Integer> tableIdIterator() { //@ADDED
        return id_file.keySet().iterator();
    }

    public String getTableName(int id) { //@ADDED
        String name = id_name.get(id);
        
        if(name == null)
            throw new NoSuchElementException();

        return name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() { //@ADDED
        id_file.clear();
        id_name.clear();
        id_pkey.clear();
        name_file.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        File cFile = new File(catalogFile); //@ADDED

        String line = "";
        //String baseFolder=new File(catalogFile).getParent();
        String baseFolder = cFile.getAbsoluteFile().getParent(); //@ADDED
        try {
            //BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            BufferedReader br = new BufferedReader(new FileReader(cFile)); //@ADDED
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

