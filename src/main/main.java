package main;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class main {

    public static void main(String[] args) {

        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);

        String uri = "mongodb+srv://admin:admin@tp3laboratorio-cxayj.mongodb.net/test?retryWrites=true&w=majority";
        MongoClientURI clientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(clientURI);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("paises_db");
        MongoCollection<Document> collection = mongoDatabase.getCollection("paises");

        Document document = new Document();

        String datosJson = "https://restcountries.eu/rest/v2/callingcode/";

        JSONParser parser = new JSONParser();

        for (int i = 1; i <= 300; i++) {

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
        System.out.println("\nPaíses con región Americas: ");
        imprimeAmericas(collection);
        System.out.println("\nPaises con región Americas y población mayor a 100000000");
        regionPoblacion(collection);
        System.out.println("\nPaises con región distinta a Africa");
        neAfrica(collection);
        System.out.println("\nBuscando Egypt para actualizar...");
        updateEgypt(collection);
        System.out.println("\nBuscando código 258 para eliminarlo de la colección...");
        delete258(collection);
        System.out.println("\nPoblación mayor que 50000000 y menor que 150000000");
        poblacionMayorqueMenorque(collection);
        System.out.println("\nPaises ordenado por nombre (Ascendente): ");
        ordenadoPorNombre(collection);

        System.out.println("\nConvirtiendo a codigoPais en INDEX");
        try {
            collection.createIndex(Indexes.ascending("codigoPais"));

            System.out.println("Index creado para codigoPais");
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

    }

    public static void imprimeAmericas(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("region", "Americas");

        MongoCursor<Document> cursor = collection.find(query).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }

    public static void regionPoblacion(MongoCollection collection) {

        BasicDBObject criteria = new BasicDBObject();
        criteria.put("region", "Americas");
        criteria.put("poblacion", new Document("$gt", 50000000));

        MongoCursor<Document> cursor = collection.find(criteria).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

    }

    public static void neAfrica(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.append("region", new BasicDBObject("$ne", "Africa"));

        MongoCursor<Document> cursor = collection.find(query).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }
    }

    public static void updateEgypt(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("nombrePais", "Egypt");

        Document found = (Document) collection.find(new Document("nombrePais", "Egypt")).first();

        if (found != null) {

            Bson update = new Document("nombrePais", "Egipto").append("poblacion", 95000000);
            Bson updateEgypt = new Document("$set", update);
            collection.updateOne(found, updateEgypt);
            System.out.println("Egypt actualizado");

        }

    }

    public static void delete258(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("codigoPais", 258);
        DeleteResult result = collection.deleteOne(query);
        System.out.println("Resultado de la operación: " + result.getDeletedCount());

    }

    public static void poblacionMayorqueMenorque(MongoCollection collection) {

        FindIterable<Document> iterable = collection.find(
                new Document("poblacion", new Document("$gt", 50000000).append("$lt", 150000000)));

        MongoCursor<Document> cursor = iterable.iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }

    public static void ordenadoPorNombre(MongoCollection collection) {

        MongoCursor<Document> cursor = collection.find().sort(new BasicDBObject("nombrePais", 1)).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }

}
