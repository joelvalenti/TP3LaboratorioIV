package main;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class main {

    
    public static void main(String[] args) {
        
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE); 

        String uri = "mongodb+srv://admin:admin@tp3laboratorio-cxayj.mongodb.net/test";
        MongoClientURI clientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(clientURI);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("paises_db");
        MongoCollection collection = mongoDatabase.getCollection("paises");
        
        Document document = new Document();
        
        String datosJson = "https://restcountries.eu/rest/v2/callingcode/";
        
        JSONParser parser = new JSONParser();
        
        for (int i = 1; i <= 2; i++) {
            
            try {
                
                URL link = new URL(datosJson + i);
                URLConnection yc = link.openConnection();
                
                BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
                
                JSONArray arrjson = (JSONArray) parser.parse(in.readLine());
                if (arrjson != null) {
                    for (Object object : arrjson) {
                        JSONObject paisJson = (JSONObject) object;
                        document.append("codigoPais", i);
                        document.append("nombrePais", paisJson.get("name"));
                        document.append("capitalPais", paisJson.get("capital"));
                        document.append("region", paisJson.get("region"));
                        document.append("poblacion", paisJson.get("population"));
                        List coorGeo = (List) paisJson.get("latlng");
                        document.append("latitud", (double) coorGeo.get(0));
                        document.append("longitud", (double) coorGeo.get(1));
                        document.append("superficie", paisJson.get("area"));
                        collection.insertOne(document);
                        document.clear();
                        System.out.println("Pais encontrado, código: " + i);
                    }
                } else {
                    continue;
                }
                in.close();
            } catch (Exception e) {
                System.out.println("No existe un pais con el código: " + i);
            }
        }
        System.gc();
    }
}
