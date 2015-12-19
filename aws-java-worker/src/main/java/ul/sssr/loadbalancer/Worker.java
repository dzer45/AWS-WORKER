package ul.sssr.loadbalancer;

import java.util.List;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
					e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region euCentral1 = Region.getRegion(Regions.EU_CENTRAL_1);
		sqs.setRegion(euCentral1);
		
		String myQRequestUrl = sqs.getQueueUrl("arif-QRequest").getQueueUrl();
		
		while (true) {
			List<Message> msgs = sqs.receiveMessage(
					new ReceiveMessageRequest(myQRequestUrl)
							.withMaxNumberOfMessages(1)).getMessages();

			if (msgs.size() > 0) {
				Message message = msgs.get(0);
				String data = message.getBody();
				System.out.println("Le message est " +data );
				
				// split le message et récupèrer la val et k
				
				String[] valAndK = data.split(" ");
				int n = Integer.parseInt(valAndK[0]);
				int k = Integer.parseInt(valAndK[1]);

				System.out.println("On a n = "+n);
				System.out.println("On a k = "+k);
				// Récupération queue response k

				String myResponseUrl = "";
				while(sqs.getQueueUrl("arif-QResponse-" + k) == null){
					myResponseUrl = sqs.getQueueUrl("arif-QResponse-" + k)
						.getQueueUrl();
					System.out.println(""+sqs.getQueueUrl("arif-QResponse-" + k));
				}
				// calcul fib
				
				int val = fib(n);

				System.out.println("Le résultat dans le worker est "+val);
				// Send a message
				sqs.sendMessage(new SendMessageRequest(myResponseUrl, "" + val));
				
				sqs.deleteMessage(new DeleteMessageRequest(myQRequestUrl,message.getReceiptHandle()));
			}
		}
	}

	public static int fib(int i) {
		if (i <= 1)
			return 1;
		else
			return fib(i - 1) + fib(i - 2);
	}
}