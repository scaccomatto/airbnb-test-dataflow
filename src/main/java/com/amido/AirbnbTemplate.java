package com.amido;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.options.Validation.Required;

import java.util.List;

public class AirbnbTemplate {

    public static void main(String[] args) {

        AirbnbTemplateOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AirbnbTemplateOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("GetPriceById", ParDo.of(new DoFn<String, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String[] airbnbPorperty = c.element().split(",");
                        if(airbnbPorperty.length>=9 && airbnbPorperty[9].matches("\\d+")){
                            c.output(KV.of(airbnbPorperty[0],Integer.valueOf(airbnbPorperty[9])));
                        }

                    }
                }))
                .apply("Top_5", Top.of(5, new KV.OrderByValue<>())) //
                .apply("ToString", ParDo.of(new DoFn<List<KV<String, Integer>>, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        StringBuffer sb = new StringBuffer();
                        for (KV<String, Integer> kv : c.element()) {
                            sb.append(kv.getKey() + "," + kv.getValue() + '\n');
                        }
                        c.output(sb.toString());
                    }

                })) //
                .apply(TextIO.write().to(options.getOutput()+"top5"));

        pipeline.run().waitUntilFinish();

    }


    public interface AirbnbTemplateOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://demo_aribnb_londra/data/listings.csv")
        String getInputFile();

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */

        //@Default.String("gs://demo_aribnb_londra/output/")
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }

}
