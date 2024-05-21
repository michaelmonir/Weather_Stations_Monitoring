package org.example.weatherstation;


import org.example.kafka.Producer;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class OpenMeteoWeatherStation {
    private static final String API_URL = "https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s&current=temperature_2m,relative_humidity_2m,wind_speed_10m&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1";

    private final HttpClient httpClient;
    private final Utilities utilities;
    private final Producer producer;


    public OpenMeteoWeatherStation() {
        this.httpClient = HttpClient.newHttpClient();
        this.utilities = new Utilities();
        this.producer = new Producer();
    }

    public String getWeather(double latitude, double longitude) throws InterruptedException, ExecutionException {
        String urlToRead = String.format(API_URL, latitude, longitude);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlToRead))
                .build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response = responseFuture.get();
        return response.body();
    }


    public WeatherStatusMsg ExtractWeatherStatusMsg(String weatherJson){
        JSONObject weatherData = new JSONObject(weatherJson);
        long time = weatherData.getJSONObject("current").getInt("time");
        double temperature = weatherData.getJSONObject("current").getDouble("temperature_2m");
        int relative_humidity = weatherData.getJSONObject("current").getInt("relative_humidity_2m");
        double wind_speed = weatherData.getJSONObject("current").getDouble("wind_speed_10m");

        return new WeatherStatusMsg(utilities.getRandomStationId(), utilities.getNextSequenceNumber(), utilities.generateBatteryStatus(), time, new Weather(relative_humidity, (int)temperature, (int)wind_speed));
    }

    public String getWeatherStatusMsg() throws Exception {
        double latitude = utilities.getRandomNoBetweenXAndNegativeX(90);
        double longitude = utilities.getRandomNoBetweenXAndNegativeX(180);
        String weatherJson = getWeather(latitude, longitude);
        return utilities.changeMsgToJson(ExtractWeatherStatusMsg(weatherJson));
    }
    public void send() throws Exception {
        Long startTime = System.currentTimeMillis();
        String temp = getWeatherStatusMsg();
        if (temp != null) producer.send(temp);
        Long endTime = System.currentTimeMillis();
        long apiCallTime = endTime - startTime;
        if (apiCallTime < 1000)
            Thread.sleep(1000 - apiCallTime);
        else
            System.out.println("API call took longer than 1 second");
    }

//    public static void main(String[] args) throws Exception {
//        OpenMeteoWeatherStation openMeteoWeather = new OpenMeteoWeatherStation();
//        while(true){
//            openMeteoWeather.send();
//        }
//    }

}
