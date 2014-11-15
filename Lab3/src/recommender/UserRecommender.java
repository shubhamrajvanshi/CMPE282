package recommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * @author shubham
 *
 */

public class UserRecommender {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DataModel model = new FileDataModel(new File("data/ratings.csv"));
			
			UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		//	ItemSimilarity item = new LogLikelihoodSimilarity(model);
		//	TanimotoCoefficientSimilarity item = new TanimotoCoefficientSimilarity(model);
			
		//	UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(20, similarity, model);
			
			UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		//	GenericItemBasedRecommender gm = new GenericItemBasedRecommender(model, item);

	//		int x=1;
			BufferedWriter bw = new BufferedWriter(new FileWriter("output/userrecommendation.csv"));
			
			for(LongPrimitiveIterator prim = model.getUserIDs(); prim.hasNext();){
				long userid = prim.nextLong();
	//			System.out.println(userid);
				List<RecommendedItem> userrecommendation = recommender.recommend(userid, 3);
				
				for(RecommendedItem recommendation : userrecommendation){
//					System.out.println(userid+ " "+ recommendation);
					System.out.println(userid+","+recommendation.getItemID()+","+recommendation.getValue());
					bw.write(userid+","+recommendation.getItemID()+","+recommendation.getValue()+"\n");
				}
//				x++;	
//				//if(x>10) System.exit(1);
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("There was an error.");
			e.printStackTrace();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			System.out.println("There was a taste error.");
			e.printStackTrace();
		}

	}

}
