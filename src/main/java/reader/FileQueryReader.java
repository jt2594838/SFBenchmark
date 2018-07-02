package reader;

import entity.Query;

import java.util.Iterator;

public class FileQueryReader implements QueryReader {

    // TODO : fix this
    @Override
    public Iterator<Query> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Query next() {
        return null;
    }
}
