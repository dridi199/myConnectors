package fr.edf.dco.common.connector.server;

import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.misc.ElasticsearchHttpConnector;
import org.json.JSONException;
import org.json.JSONObject;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class Test {

    private static final DateTimeFormatter pivot = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static ZonedDateTime fromPivot(String d) {
        return ZonedDateTime.parse(d, pivot);
    }

    public static String deleteMyQuery() {
        String id = "BOMBYX100000000c002b1808f40e2";
        String query = "{\"query\" : {\"bool\" : { \"must_not\": {\"exists\": {\"field\": \"date_fin\" }},\"must\": [ {\"query_string\": {\"default_field\": " +
                "\"_identifiant\", \"query\": \"" + id + "\"}}]}}}";

        return query;
    }


    public static void main(String[] args) {
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("identifiant", "009902b0f796f6b1");
        m1.put("si_en_cours", "1");
        m1.put("nni", "A00104");
        m1.put("_idActivite", "1009601");
        m1.put("date_debut_traite", "2018-01-04T12:38:50+01:00");
        m1.put("activite", "DEM_ACI");
        m1.put("si_en_cours", "1");
        m1.put("dcr", "MA SUO");
        m1.put("uc_conseiller", "4");
        m1.put("crc", "RC MA SUO");
        m1.put("_identifiant", "A001041009601009902b0f796f6b1");
        m1.put("uo_conseiller", "MA");
        m1.put("equipe", "RC MA SUO APU");
        m1.put("_timeInsert", "2018-01-04T12:42:03.43+01:00");
        m1.put("type", "traite_voix");


        System.out.println("test");
        String es = "noeyy3pu.noe.edf.fr,noeyycgd.noe.edf.fr,noeyycge.noe.edf.fr";
        String index = "hypster_organisation";
        String type = "evenements";


        ElasticsearchHttpConnector conn = new ElasticsearchHttpConnector(es, 29212, "elastic", "changeme");


        try {
            conn.connect();
        } catch (ConnectorException e) {
            e.printStackTrace();
        }

        String q = deleteMyQuery();
        conn.deleteBySpecificQuery(index, type, q);


        JSONObject jo = new JSONObject(m1);

        conn.put(index, type, jo);

        String query = "{\"query\":{\"bool\":{\"must\":[{\"query_string\":{\"default_field\":\"nni\", \"query\":\"BOMBYX\" }},{\"query_string\":{\"default_field\":\"si_en_cours\", \"query\":\"1\" }}]}}}";

        List<String> existEvents = conn.searchListWithMeta(index, type, query);

        if(existEvents != null) {
            System.out.println(existEvents);
            for (String event : existEvents) {
                String chose = "{\"" + event.replaceAll("=", "\":\"").replaceAll(",", "\",\"") + "\"}";

                try {
                    JSONObject jj = new JSONObject(chose);
                    String date_fin = "2018-01-04T12:42:03.449+01:00";
                    String newS = (jj.has("date_debut") ? jj.get("date_debut").toString() : null);

                    jj.put("si_en_cours", "0");
                    jj.put("date_fin", date_fin);

                    jj.remove("es_metadata_id");
                    jj.remove("_id");

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
    //search

        query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"query_string\": {\n" +
                "            \"default_field\": \"categorie_activite\",\n" +
                "            \"query\": \"FO\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        List<String> list = conn.searchListWithMeta("hypster_organisation","evenements",query);

        if(list != null){
            System.out.println("site list " + list.size());
        }

        query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"query_string\": {\n" +
                "            \"default_field\": \"categorie_activite\",\n" +
                "            \"query\": \"iuzeahdzaeiohazeodfhofzea\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        list = conn.searchListWithMeta("hypster_organisation","evenements",query);

        if(list != null){
            System.out.println("site list " + list.size());
        }


        String health = conn.checkHealthClusterStatus();
        String healthWithIndex = conn.checkHealthWithIndexClusterStatus("hypster_organisation");

        System.out.println("health : "+health + " with index : " +healthWithIndex);

    }
}