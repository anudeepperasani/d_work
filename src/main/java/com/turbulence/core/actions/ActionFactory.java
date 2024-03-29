package com.turbulence.core.actions;

import java.io.InputStream;

import java.net.URI;

public class ActionFactory {
    public static RegisterSchemaAction createRegisterSchemaAction(URI schemaURI) {
        return new RegisterSchemaAction(schemaURI);
    }

    public static StoreDataAction createStoreDataAction(InputStream in) {
        return new StoreDataAction(in);
    }

    public static StoreXMLDataAction createStoreXMLDataAction(InputStream in) {
        return new StoreXMLDataAction(in);
    }

    public static QueryAction createQueryAction(String query) {
        return new QueryAction(query);
    }
    public static QueryAction1 createQueryAction1(String query) {
        return new QueryAction1(query);
    }

    public static LUBMQueryAction createLUBMQueryAction(String query) {
        return new LUBMQueryAction(query);
    }
}
