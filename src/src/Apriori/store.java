package src.Apriori;

public class store {
    private String value;
    private store next;
    private int len;

    public store()
    {
        value="";
        next=null;
        len=0;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public store getNext() {
        return next;
    }

    public void setNext(store next) {
        this.next = next;
        len++;
    }

    public boolean isEnd()
    {
        if(next==null)
        {
            return false;
        }
        return true;
    }

    public void put_node(store next)
    {
        while (next!=null)
        {
            store temp = next.next;
        }
    }


}
