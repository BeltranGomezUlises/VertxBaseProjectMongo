/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import io.vertx.core.json.JsonObject;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils for valitation of data
 *
 * @author kriblet
 */
public class UtilsValidation {

    public static final String MISSING_REQUIRED_VALUE = "Missing required value";
    public static final String INVALID_FORMAT = "invalid format";
    public static final String INVALID_PARAMETER = "invalid parameter";
    public static final String PARAMETER_DOES_NOT_EXIST = "field does not exist";
    public static final String ALREADY_EXISTS = "field already exists";

    private static final SimpleDateFormat SDF_DATETIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    private static final String PHONE_NUMBER_PATTERN = "^[0-9]*$";
    private static final String SPECIAL_CHARS = "/*!@#$%^&*()\"{}_[]|\\?/<>,.";
    private static final String TIME24HOURS_PATTERN = "([01]?[0-9]|2[0-3]):[0-5][0-9]";
    private static final String DATE = "^((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$";
    private static final String HAS_NUMBER = ".*\\d+.*";

    /**
     * Validates that the property propertyName in the JsonObject object is an email with the pattern
     * "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$", ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isMail(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluate(EMAIL_PATTERN, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a phone number with the pattern: "^[0-9]*$",
     * ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isPhoneNumber(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluate(PHONE_NUMBER_PATTERN, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a date with the pattern:
     * "^((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$" , ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isDate(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluate(DATE, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a hour with the pattern:
     * "([01]?[0-9]|2[0-3]):[0-5][0-9]", ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isHour24(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluate(TIME24HOURS_PATTERN, object, propertyName);
    }

    /**
     * Validates that the text contains special character and do not be empty, ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void containsSpecialCharacter(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s != null) {
                if (s.isEmpty()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                for (int i = 0; i < s.length(); i++) {
                    if (SPECIAL_CHARS.contains(s.substring(i, (i + 1)))) {
                        throw new PropertyValueException(propertyName, INVALID_FORMAT);
                    }
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }

    }

    /**
     * Validates that the property matches with a personal name, ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isName(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s != null) {
                if (s.isEmpty()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                if (s.contains("\"") || s.contains("'")) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                Matcher matcher = Pattern.compile(HAS_NUMBER).matcher(s);
                if (matcher.matches()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }

    }

    /**
     * Validates that the int value in the property is between the range from-to including from and to values, ignoring
     * null
     *
     * @param from initial value to start the range
     * @param to final value to end the range
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void betweenRange(final JsonObject object, final String propertyName, final int from, final int to) throws PropertyValueException {
        try {
            Integer val = object.getInteger(propertyName);
            if (val != null) {
                if (val < from || val > to) { //out of range
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }

    }

    /**
     * Validates that the value in the property is one of the values provided, ignoring null
     *
     * @param values values to check equality
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isContained(final JsonObject object, final String propertyName, String... values) throws PropertyValueException {
        try {
            String propertyValue = object.getString(propertyName);
            if (propertyValue != null) {
                for (String value : values) {
                    if (value.equals(propertyValue)) {
                        return;
                    }
                }
                throw new PropertyValueException(propertyName, INVALID_FORMAT);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }

    }

    /**
     * Validates that the property propertyName in the JsonObject object is an email with the pattern
     * "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$"
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isMailAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluateAndNotNull(EMAIL_PATTERN, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a phone number with the pattern: "^[0-9]*$"
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isPhoneNumberAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluateAndNotNull(PHONE_NUMBER_PATTERN, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a date with the pattern:
     * "^((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$"
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isDateAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluateAndNotNull(DATE, object, propertyName);
    }

    /**
     * Validates that the property propertyName in the JsonObject object is a hour with the pattern:
     * "([01]?[0-9]|2[0-3]):[0-5][0-9]"
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isHour24AndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        evaluateAndNotNull(TIME24HOURS_PATTERN, object, propertyName);
    }

    /**
     * Validates that the text contains special character and do not be empty, considering not null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void containsSpecialCharacterAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s == null) {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
            if (s.isEmpty()) {
                throw new PropertyValueException(propertyName, INVALID_FORMAT);
            }
            for (int i = 0; i < s.length(); i++) {
                if (SPECIAL_CHARS.contains(s.substring(i, (i + 1)))) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validates that the property matches with a personal name
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isNameAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s != null) {
                if (s.isEmpty()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                if (s.contains("\"") || s.contains("'")) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                Matcher matcher = Pattern.compile(HAS_NUMBER).matcher(s);
                if (matcher.matches()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            } else {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validates that the int value in the property is between the range from-to including from and to values
     *
     * @param from initial value to start the range
     * @param to final value to end the range
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void betweenRangeAndNotNull(final JsonObject object, final String propertyName, final int from, final int to) throws PropertyValueException {
        try {
            Integer val = object.getInteger(propertyName);
            if (val != null) {
                if (val < from || val > to) { //out of range
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            } else {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validates that the value in the property is one of the values provided
     *
     * @param values values to check equality
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isContainedAndNotNull(final JsonObject object, final String propertyName, String... values) throws PropertyValueException {
        try {
            String propertyValue = object.getString(propertyName);
            if (propertyValue != null) {
                for (String value : values) {
                    if (value.equals(propertyValue)) {
                        return;
                    }
                }
                throw new PropertyValueException(propertyName, INVALID_FORMAT);
            } else {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validate the value of the property in the object with the pattern given as regular expresion, includes null
     * validation, ignoring null
     *
     * @param pattern pattern to compile
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    private static void evaluate(final String pattern, JsonObject object, String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s != null) {
                if (s.isEmpty()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
                Matcher matcher = Pattern.compile(pattern).matcher(s);
                if (!matcher.matches()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validate the value of the property in the object with the pattern given as regular expresion, includes null
     * validation
     *
     * @param pattern pattern to compile
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    private static void evaluateAndNotNull(final String pattern, JsonObject object, String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s == null) {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
            if (s.isEmpty()) {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
            Matcher matcher = Pattern.compile(pattern).matcher(s);
            if (!matcher.matches()) {
                throw new PropertyValueException(propertyName, INVALID_FORMAT);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validates that a string has almost 1 character
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isEmptyAndNotNull(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s == null) {
                throw new PropertyValueException(propertyName, MISSING_REQUIRED_VALUE);
            }
            if (s.isEmpty()) {
                throw new PropertyValueException(propertyName, INVALID_FORMAT);
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }
    }

    /**
     * Validates that a string has almost 1 character, ignoring null
     *
     * @param object object to evaluate
     * @param propertyName name of the property to evaluate
     * @throws utils.UtilsValidation.PropertyValueException if the evaluation fails
     */
    public static void isEmpty(final JsonObject object, final String propertyName) throws PropertyValueException {
        try {
            String s = object.getString(propertyName);
            if (s != null) {
                if (s.isEmpty()) {
                    throw new PropertyValueException(propertyName, INVALID_FORMAT);
                }
            }
        } catch (ClassCastException e) {
            throw new PropertyValueException(propertyName, INVALID_FORMAT);
        }

    }

    public static class PropertyValueException extends Exception {

        private String name;
        private String error;

        public PropertyValueException(String name, String error) {
            this.name = name;
            this.error = error;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

    }

}
