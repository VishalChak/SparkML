package ml.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TextTranslation {

	public static void main(String[] args) throws Exception {

		TextTranslation api = new TextTranslation();

		/*
		 * String url =
		 * "https://translation.googleapis.com/language/translate/v2?key=YOUR_API_KEY";
		 * 
		 * Map<String, Object> map = new HashMap<>(); map.put("key",
		 * "AIzaSyDnyO0lXDCRH9UGNpGTAJBkqiUrr_0nA4Q");
		 * 
		 * String createUrlConnectionAndGenerateResponse =
		 * api.doPostRequest(url, map);
		 * System.out.println(createUrlConnectionAndGenerateResponse);
		 */

		// String url2 =
		// "https://api.microsofttranslator.com/v2/http.svc/Translate?";
		String url2 = "https://translation.googleapis.com/language/translate/v2/detect?key=AIzaSyBkJkiAhHm-1gbQ_6bzpklQUu7xaf88Syw";

		Map<String, String> requestParam = new HashMap();
		// requestParam.put("appid", "Bearer+" +
		// createUrlConnectionAndGenerateResponse);
		requestParam.put("q", "how are you");
		// requestParam.put("source", "en");
		// requestParam.put("target", "hi");
		// requestParam.put("contentType", "text/plain");
		// requestParam.put("category", "general");

		if (requestParam != null) {
			Set<String> keySet = requestParam.keySet();
			if (keySet != null && keySet.size() > 0) {
				Boolean firstTime = true;
				for (String key : keySet) {
					String queryString = key + "=" + requestParam.get(key);
					url2 += (firstTime ? queryString : "&" + queryString);
					firstTime = false;
				}
			}
		}

		String textResult = api.doGetRequest(url2);
	}

	public String doPostRequest(String url, Map<String, Object> requestParam) throws Exception {
		URL obj = new URL(url);
		URLConnection openConnection = obj.openConnection();

		if (requestParam != null) {
			Set<String> keySet = requestParam.keySet();
			if (keySet != null && keySet.size() > 0) {
				for (String key : keySet) {
					// System.out.println(requestParam.get(key));
					openConnection.addRequestProperty(key, requestParam.get(key).toString());
				}
			}
		}
		openConnection.setDoOutput(true);

		OutputStream outputStream = openConnection.getOutputStream();

		if (outputStream != null) {
			return readInputStreamAndGenerateOutput(openConnection.getInputStream());
		}
		return null;
	}

	public String doGetRequest(String urlString) throws IOException {

		URL url = new URL(urlString);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		int code = connection.getResponseCode();
		String readInputStreamAndGenerateOutput = readInputStreamAndGenerateOutput(connection.getInputStream());
		return readInputStreamAndGenerateOutput;
	}

	public String readInputStreamAndGenerateOutput(InputStream inputStream) throws IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}

}
