package com.nitesh.springbootmultithreading.services;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.nitesh.springbootmultithreading.entities.User;

@Service
public class UserService {

    Logger LOG = LoggerFactory.getLogger(UserService.class);

    @Autowired
    private DataSource dataSource;

    // ERROR 1: Quitamos @Async. Gestionaremos los hilos manualmente y mal.
    public CompletableFuture<List<User>> saveUsers(MultipartFile file) throws Exception {
        // ERROR 1 (Hilos): Creamos un hilo manual por cada petición.
        Thread thread = new Thread(() -> {
            try {
                LOG.info("Iniciando hilo manual incontrolado: " + Thread.currentThread().getName());
                
                List<User> users = parseCsvFile(file);
                saveUsersManually(users);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        return CompletableFuture.completedFuture(new ArrayList<>());
    }

    // Método auxiliar para simular el guardado manual (JDBC) con fugas
    private void saveUsersManually(List<User> users) {
        try {
            // ERROR 2 (Base de Datos): Abrimos conexión manual
            Connection conn = dataSource.getConnection(); 
            // OJO: Al pedir connection manual, sacamos una del Pool (Hikari).
            // Como NUNCA hacemos conn.close(), esa conexión queda "secuestrada" para siempre.
            // Después de 10 peticiones (tamaño default del pool), la app se bloqueará.

            // ERROR añadido: forzamos nextval manualmente en el insert (sin usar JPA) para esquivar el null del id
            String sql = "INSERT INTO users (id, name, email, gender) VALUES (nextval('users_id_seq'), ?, ?, ?)";
            PreparedStatement ps = conn.prepareStatement(sql);

            for (User user : users) {
                ps.setString(1, user.getName());
                ps.setString(2, user.getEmail());
                ps.setString(3, user.getGender());
                ps.addBatch();
            }
            ps.executeBatch();
            
            // ERROR FATAL: ¡FALTA conn.close()!
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<User> parseCsvFile(final MultipartFile file) throws Exception {
        final List<User> users = new ArrayList<>();
        
        // ERROR 3 (Archivos): NO usamos try-with-resources
        // El BufferedReader se crea y se queda en el limbo de la memoria.
        BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()));
        
        String line;
        boolean firstLine = true;
        while ((line = br.readLine()) != null) {
            // Saltamos la primera línea (encabezados del CSV)
            if (firstLine) {
                firstLine = false;
                continue;
            }
            
            final String[] data = line.split(",");
            final User user = new User();
            // El CSV tiene: name,email,gender
            user.setName(data[0]);
            user.setEmail(data[1]);
            user.setGender(data[2]);
            users.add(user);
        }
        
        // ERROR FATAL: ¡FALTA br.close()!
        // El archivo queda abierto en el Sistema Operativo.
        
        return users;
    }
    
    // ERROR 4 (Acceso a BD): findAll con mala práctica - conexión manual sin cierre
    public List<User> findAll() {
        List<User> users = new ArrayList<>();
        try {
            // ERROR: Conexión manual sin try-with-resources
            // y sin llamar a close() - causará fuga de conexión
            Connection conn = dataSource.getConnection();
            
            String sql = "SELECT id, name, email, gender FROM users";
            PreparedStatement ps = conn.prepareStatement(sql);
            
            var resultSet = ps.executeQuery();
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getInt("id"));
                user.setName(resultSet.getString("name"));
                user.setEmail(resultSet.getString("email"));
                user.setGender(resultSet.getString("gender"));
                users.add(user);
            }
            
            // ERROR FATAL: ¡FALTA resultSet.close(), ps.close() Y conn.close()!
            // Estas fugas de recursos causarán un bloqueo eventual de la aplicación.
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return users;
    }
}