package com.putti.pubsubpublisher;

import java.util.concurrent.TimeUnit;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class MainClass {

	private static String PROJECT_ID = "putti-project2";
	private static String TOPIC_NAME = "my-topic";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Test PubSub Publisher");
		ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, TOPIC_NAME);
		
		Publisher publisher = null;		
		
		try {
			publisher = Publisher.newBuilder(topicName).build();
			
			int i = 0;
			
			for (i = 0; i < 2; i++) {
				final String message = createJson();
				ByteString data = ByteString.copyFromUtf8(message);
				System.out.println("> publishing '" + message + "' to topics:" + TOPIC_NAME);
				
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
				ApiFuture<String> future = publisher.publish(pubsubMessage);
				
			    ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
			    	@Override
			    	public void onFailure(Throwable throwable) {
			            if (throwable instanceof ApiException) {
			                ApiException apiException = ((ApiException) throwable);
			                // details on the API exception
			                System.out.println(apiException.getStatusCode().getCode());
			                System.out.println(apiException.isRetryable());
			              }
			              System.out.println("Error publishing message : " + message);		    		
			    	}

					@Override
					public void onSuccess(String arg0) {
						
						System.out.println("success, messageId: " + arg0);
					}

			    });				
			}
				
			publisher.shutdown();
			System.out.println("> published");
			
		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		} finally {

						
		}
	}
	
	private static String createJson() {
		String str = null;
		
		Gson gson = new Gson();
		DataA d = new DataA();
		d.id = "1234";
		d.name = "putti";
		
		str = gson.toJson(d);
		
		return(str);
	}

}
