package com.cloudcomputing.samza.ny_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.json.JSONObject;


import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, String> driverloc;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverloc = (KeyValueStore<String, String>)context.getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a
        // particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a
        // blockId will arrive
        // at one task only, thereby enabling you to do stateful stream
        // processing.
        try {
            String incomingStream = envelope.getSystemStreamPartition().getStream();

            if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
                // Handle Driver Location messages
                processDriverLoc((Map<String, Object>) envelope.getMessage());
            } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
                // Handle Event messages
                processEvent((Map<String, Object>) envelope.getMessage(),collector);
            } else {
                throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private void processDriverLoc(Map<String, Object> message){
        try {
            int driverId = (int) message.get("driverId");
            int blockId = (int) message.get("blockId");
            double latitude = (double) message.get("latitude");
            double longitude = (double) message.get("longitude");
            String key = blockId + ":" + driverId;
            //record blockID-DriverID's up-to-date location
            if (driverloc.get(key) != null) {
                JSONObject jsonObject = new JSONObject(driverloc.get(key));
                jsonObject.put("latitude", latitude);
                jsonObject.put("longitude", longitude);
                driverloc.put(key, jsonObject.toString());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private void processEvent(Map<String, Object> message, MessageCollector collector) {

        String type = (String) message.get("type");
        if(type.equals("ENTERING_BLOCK")) {
            try{
                int driverId = (int)message.get("driverId");
                int blockId = (int)message.get("blockId");
                String status = (String)message.get("status");
                String key = blockId + ":" + driverId;
                if(status.equals("AVAILABLE")) {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("latitude",(double)message.get("latitude"));
                    jsonObject.put("longitude",(double)message.get("longitude"));
                    jsonObject.put("gender",message.get("gender"));
                    jsonObject.put("rating",message.get("rating"));
                    jsonObject.put("salary",message.get("salary"));
                    driverloc.put(key, jsonObject.toString());
                } else {
                    //driverloc.delete(key);
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }

        } else if (type.equals("LEAVING_BLOCK")) {
            try {
                int driverId = (int)message.get("driverId");
                int blockId = (int)message.get("blockId");
                String key = blockId + ":" + driverId;
                String status = (String)message.get("status");
                driverloc.delete(key);
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }

        } else if (type.equals("RIDE_REQUEST")) {
            try {
                int clientId = (int)message.get("clientId");
                int blockId = (int)message.get("blockId");
                double latitude = (double)message.get("latitude");
                double longitude = (double)message.get("longitude");
                String gender_preference = (String)message.get("gender_preference");

                KeyValueIterator<String, String> result = driverloc.range(blockId + ":", blockId + ":99999999");
                int matchDriverId = -1;
                double highestScore = Double.MIN_VALUE;

                while(result.hasNext()) {
                    Entry<String, String> entry = result.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    int driverId = Integer.parseInt(key.split(":")[1]);
                    JSONObject jsonObject = new JSONObject(value);
                    if(jsonObject.has("rating")) {
                        double driver_la = jsonObject.getDouble("latitude");
                        double driver_lo = jsonObject.getDouble("longitude");
                        double rating = jsonObject.getDouble("rating");
                        String gender = jsonObject.getString("gender");
                        double genderScore = 0.0;
                        if (gender_preference.equals("N") || gender_preference.equals(gender)) {
                            genderScore = 1.0;
                        }
                        int salary = jsonObject.getInt("salary");
                        double current_score = calculateScore(driver_la,driver_lo,latitude,longitude,rating,genderScore,salary);
                        System.out.println("*****current_score******"+String.valueOf(current_score));
                        if(highestScore < current_score) {
                            highestScore = current_score;
                            matchDriverId = driverId;
                        }
                    }
                }
                result.close();

                if(matchDriverId != -1) {
                    driverloc.delete(blockId + ":" + matchDriverId);
                }

                Map<String, Object> send = new HashMap<String, Object>();
                send.put("clientId", clientId);
                send.put("driverId", matchDriverId);
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, send));
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }


        } else if (type.equals("RIDE_COMPLETE")) {
            try {
                int driverId = (int)message.get("driverId");
                int blockId = (int)message.get("blockId");
                // Update the location and personal information
                String key = blockId + ":" + driverId;
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("latitude",(double)message.get("latitude"));
                jsonObject.put("longitude",(double)message.get("longitude"));
                jsonObject.put("gender",message.get("gender"));
                double rating = (double) message.get("rating");
                double user_rating = (double) message.get("user_rating");
                jsonObject.put("rating",(rating+user_rating)/2);
                jsonObject.put("salary",message.get("salary"));
                driverloc.put(key, jsonObject.toString());
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }


        }
    }

    private double calculateScore(double driver_la, double driver_lo, double client_la, double client_lo, double rating, double gender_score, int salary) {
        double client_driver_distance = Math.sqrt(Math.pow((client_la - driver_la), 2) + Math.pow((client_lo - driver_lo), 2));
        double distance_score = Math.exp(client_driver_distance*(-1));
        double rating_score = rating/5.0;
        double salary_score = 1.0 -salary/100.0;
        double match_score = distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2;
        return match_score;
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }
}
