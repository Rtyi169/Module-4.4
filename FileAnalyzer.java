import java.io.File;
import java.sql.*;

public class FileAnalyzer {
    private static final String DATABASE_URL = "jdbc:derby:fileAnalyzerDB;create=true";

    public static void main(String[] args) {
        FileAnalyzer fileAnalyzer = new FileAnalyzer();

        // Specify the directory path to analyze
        String directoryPath = "path/to/directory";

        // Create tables and add files to the database
        fileAnalyzer.createTables();
        fileAnalyzer.addFilesToDatabase(directoryPath);

        // Display a list of tables in the database
        fileAnalyzer.displayTables();

        // Display files from the chosen directory
        fileAnalyzer.displayFiles(directoryPath);
    }

    public void createTables() {
        try (Connection conn = DriverManager.getConnection(DATABASE_URL);
             Statement stmt = conn.createStatement()) {
            // Create a table for storing file information
            stmt.execute("CREATE TABLE IF NOT EXISTS files (" +
                    "id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                    "name VARCHAR(255), " +
                    "path VARCHAR(255), " +
                    "extension VARCHAR(10), " +
                    "size BIGINT)");

            System.out.println("Tables created successfully!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addFilesToDatabase(String directoryPath) {
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                // Add file information to the database
                String name = file.getName();
                String path = file.getAbsolutePath();
                String extension = getFileExtension(name);
                long size = file.length();

                String sql = "INSERT INTO files (name, path, extension, size) VALUES (?, ?, ?, ?)";
                try (Connection conn = DriverManager.getConnection(DATABASE_URL);
                     PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, name);
                    pstmt.setString(2, path);
                    pstmt.setString(3, extension);
                    pstmt.setLong(4, size);
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Files added to the database successfully!");
        }
    }

    public void displayTables() {
        try (Connection conn = DriverManager.getConnection(DATABASE_URL);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE='T'");

            System.out.println("Tables in the database:");
            while (rs.next()) {
                String tableName = rs.getString("TABLENAME");
                System.out.println(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void displayFiles(String directoryPath) {
        try (Connection conn = DriverManager.getConnection(DATABASE_URL);
             Statement stmt = conn.createStatement()) {
            String sql = "SELECT name, path, extension, size FROM files";
            ResultSet rs = stmt.executeQuery(sql);

            System.out.println("Files in the chosen directory:");
            while (rs.next()) {
                String name = rs.getString("name");
                String path = rs.getString("path");
                String extension = rs.getString("extension");
                long size = rs.getLong("size");

                System.out.printf("Name: %s, Path: %s, Extension: %s, Size: %d bytes%n",
                        name, path, extension, size);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String getFileExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf(".");
        if (dotIndex != -1) {
            return fileName.substring(dotIndex + 1).toLowerCase();
        }
        return "";
    }
}