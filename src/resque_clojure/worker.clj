(ns resque-clojure.worker
  (:use [clojure.string :only [split]]))

(defn lookup-fn [namespaced-fn]
  (let [[namespace fun] (split namespaced-fn #"/")]
    (ns-resolve (symbol namespace) (symbol fun))))

(defn work-on [state job queue]
  (let [{namespaced-fn :class args :args} job]
    (try
      (apply (lookup-fn namespaced-fn) args)
      {:result :pass :job job :queue queue}
      (catch Exception e
        {:result :error :exception e :job job :queue queue})))






;; FAILED jobs look like this in resque

;; {
;;   "failed_at":"2011/08/03 13:23:14",
;;   "payload":{
;;     "class":"TranscriptGenerator",
;;     "args":[
;;       "30f18730a023012e35185a558dfcff6b",
;;       {
;;         "transcript":{
;;           "student_id":"123-456789",
;;           "backer":"/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/custom/backers/accounts/229/backer.pdf",
;;           "student_dob":"",
;;           "issued_to_student":false,
;;           "sender":{
;;             "disable_student_watermark":false,
;;             "overlay_horizontal_offset":0,
;;             "address":{
;;               "city_state_zip":"State College, PA  16801",
;;               "line_1":"205 Fairfield Drive",
;;               "line_2":""
;;             },
;;             "name":"SCRIP-SAFE Sender",
;;             "overlay_vertical_offset":0,
;;             "contact":"SCRIP-SAFE Sender",
;;             "phone_number":"123-456-7890",
;;             "overlay_overlays":false,
;;             "accreditation_name":"Middle States Association of Colleges and Schools (MSA)",
;;             "catalog_url":"academiccatalog.com/index.php"
;;           },
;;           "output_path":"/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/transcripts/completed-transcript-17764.pdf",
;;           "mailing_page_path":"/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/transcripts/mailing-cover-page-and-documents-transcript-17764.pdf",
;;           "documents":[
;;             "/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/documents/000/000/432/432_test_spaces.pdf"
;;           ],
;;           "student_email":"",
;;           "id":17764,
;;           "type":"Transcript",
;;           "student_name":"Over the Rainbow",
;;           "input_path":"/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/transcripts/transcript-17764.pdf",
;;           "receiver":{
;;             "address":{
;;               "city_state_zip":"Cityname, NV  12345",
;;               "line_1":"Street 1 goes here",
;;               "line_2":"Street 2 goes here"
;;             },
;;             "name":"Undergraduate Admission"
;;           },
;;           "watermark":"/home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/custom/watermarks/accounts/229/watermark.PNG",
;;           "print_only":false,
;;           "document_label":"academic transcript",
;;           "overlay":null,
;;           "cover_page":"/tmp/cover-page.pdf.11118.55554"
;;         }
;;       }
;;     ]
;;   },
;;   "exception":"PdfMachine::BuildError",
;;   "error":"No such input file /home/ecdeploy/apps/scripsafe/preprod/releases/20110803171855/pdf/documents/000/000/432/432_test_spaces.pdf",
;;   "backtrace":[
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/operation/cat.rb:12:in `run!'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:22:in `run'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:83:in `visit'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:81:in `visit'",
;;     "org/jruby/RubyFixnum.java:256:in `times'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:80:in `visit'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:81:in `visit'",
;;     "org/jruby/RubyFixnum.java:256:in `times'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:80:in `visit'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/bundler/gems/pdfmachine-6b922b1b6b5f/lib/pdfmachine/pdf_builder.rb:59:in `build!'",
;;     "./config/../lib/pdf_assembler.rb:19:in `run'",
;;     "./config/../lib/pdf_assembler.rb:15:in `electronic'",
;;     "./config/../lib/workers/transcript_generator.rb:17:in `perform'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/gems/resque-status-0.2.3/lib/resque/job_with_status.rb:111:in `safe_perform!'",
;;     "/home/ecdeploy/apps/scripsafe_transcript_processor/master/shared/bundle/jruby/1.8/gems/resque-status-0.2.3/lib/resque/job_with_status.rb:88:in `perform'"
;;   ],
;;   "worker":"ess-PreProd:11118:transcript_processor",
;;   "queue":"transcript_processor"
;; }
)