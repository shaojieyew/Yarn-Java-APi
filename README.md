# Yarn API wrapper in Java


```
YarnAppQuery.builder("http://localhost","8088")
                .setApplicationId("application_1610802627554_0004")
                .get()
                .stream().forEach(x->System.out.println(x.getId()+" "+x.getName()));

YarnAppQuery.builder("http://localhost","8088")
                .setStates("RUNNING")
                .setQueue("default")
                .get()
                .stream().forEach(x->System.out.println(x.getId()+" "+x.getName()));

YarnAppQuery.builder("http://localhost","8088")
                .setApplicationId("application_1610802627554_0004")
                .kill();
```
