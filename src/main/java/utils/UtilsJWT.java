/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;

/**
 *
 * @author kriblet
 */
public class UtilsJWT {

    private static final String STRING_KEY = "Ultra secret key change to deploy in productive, this key has to be unique";

    public static String generateSessionToken(int userId) throws JsonProcessingException {
        JwtBuilder builder = Jwts.builder();
        builder.setSubject(String.valueOf(userId));
        return builder.signWith(SignatureAlgorithm.HS512, STRING_KEY).compact();
    }

    public static int getUserIdFrom(String token) {
        return Integer.parseInt(Jwts.parser().setSigningKey(STRING_KEY).parseClaimsJws(token).getBody().getSubject());
    }

    public static boolean isTokenValid(String token) {
        try {
            Jwts.parser().setSigningKey(STRING_KEY).parseClaimsJws(token);
            return true;
        } catch (ExpiredJwtException | MalformedJwtException | SignatureException | UnsupportedJwtException | IllegalArgumentException | NullPointerException e) {
            return false;
        }
    }

}
