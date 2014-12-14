package main.scala;

public class MainApp {

	public static void main(String[] args) {
		String tmp[] = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			tmp[i - 1] = args[i];
		}
		if (args[0].equalsIgnoreCase("Project4Server")) {
			Project4Server.main(tmp);
		} else if (args[0].equalsIgnoreCase("Project4Client")) {
			Project4Client.main(tmp);
		} else if (args[0].equalsIgnoreCase("HttpServer")) {
			HttpServer.main(tmp);
		}
	}

}
