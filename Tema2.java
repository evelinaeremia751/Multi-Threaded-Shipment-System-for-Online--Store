import java.io.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

class Worker implements Runnable {
    Tema2 origin;
    String orderID;
    Worker(Tema2 origin, String orderID) {
        this.origin = origin;
        this.orderID = orderID;
    }
    //daca un produs din products apartine unei comenzi din order ii punem shipped la final in orderproductswriter
    @Override
    public void run() {
        for (int i = 0; i < Tema2.comenzi.size(); i++) {
            if (Tema2.comenzi.get(i).equals(orderID)) {
                synchronized (Worker.class) {
                    try {
                        Tema2.orderProductsWriter.append(orderID).append(",").append(Tema2.produse.get(i)).append(",shipped").append("\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        origin.afiseazaComanda.set(true);
    }
}

public class Tema2 extends Thread {
    public static int P;

    public static BufferedReader ordersReader;
    public static BufferedReader orderProductsReader;
    public static final BufferedWriter ordersWriter;

    public static final BufferedWriter orderProductsWriter;
    AtomicBoolean afiseazaComanda = new AtomicBoolean(false);

    static {
        try {
            String ordersOut = "orders_out.txt";
            ordersWriter = new BufferedWriter(new FileWriter(ordersOut));//cu bufferwriter scriem in ordersout
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String orderProductsOut = "order_products_out.txt";
            orderProductsWriter = new BufferedWriter(new FileWriter(orderProductsOut));//cu bufferedwriter scriem in orderprodout
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayList<String> comenzi = new ArrayList<>();//pun comenzile intr un arraylist
    public static ArrayList<String> produse = new ArrayList<>();//pun produsele intr un arraylist
    public static CyclicBarrier barr;
    public static ExecutorService tpe;

    public static String caleFisier;

    @Override
    public void run() {
        String text;
        while (true) {
            synchronized (Tema2.class) {
                try {
                    text = orderProductsReader.readLine();//in text punem liniile din orderproducts
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (text == null) {
                break;
            }

            synchronized (Object.class) {
                String[] parti = text.split(",");//impart liniile din orderproducts intre comenzi si produse
                comenzi.add(parti[0]);//bag in lista comenzi, comenzile
                produse.add(parti[1]);//bag in lista produse, produsele
            }
        }

        try {
            barr.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        while (true) {//cat timp avem ceva de citit din fisier
            String orderID = "";
            String orderQuantity = "";
            synchronized (Tema2.class) {
                try {
                    text = ordersReader.readLine();

                    if (text == null) {
                        break;
                    }

                    String[] parti = text.split(",");//impart liniile din orders intre comenzi si nr_produse prin virgula

                    if (parti[1].equals("0")) {//daca am 0 produse
                        continue;
                    }

                    orderID = parti[0];
                    orderQuantity = parti[1];
                    if (parti[0] == null) {
                        break;
                    }
                    tpe.submit(new Worker(this, parti[0]));//??pune shipped la finalul sirului din orderproducts la liniile cu produsele dintr o anumita comanda
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

//            System.out.println(this.arr.hashCode());

            while (!afiseazaComanda.get()) {

            }

            synchronized (Object.class) {
                try {
                    ordersWriter.append(orderID + "," + orderQuantity + ",shipped" + "\n");//pune shipped la finalul fiecarei linii din orders
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            afiseazaComanda.set(false);
        }
    }

    public static void main(String[] args) {
        P = Integer.parseInt(args[1]);
        caleFisier = args[0];

        String orders = caleFisier + "/" + "orders.txt";
        String orderProducts = caleFisier + "/" + "order_products.txt";

        try {
            orderProductsReader = new BufferedReader(new FileReader(orderProducts));//citirea1
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            ordersReader = new BufferedReader(new FileReader(orders));//citirea 2
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        tpe = Executors.newFixedThreadPool(P);//pool ul cu threaduri?
        barr = new CyclicBarrier(P);

        Tema2[] t = new Tema2[P];

        for (int i = 0; i < P; i++) {
            t[i] = new Tema2();//dai drumul la threaduri
            t[i].start();
        }

        for (int i = 0; i < P; i++) {
            try {
                t[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        tpe.shutdown();

        try {
            ordersReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            orderProductsReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            ordersWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            orderProductsWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
