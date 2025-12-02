require('dotenv').config();
const axios = require('axios');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// CONFIGURATION
const API_KEY = process.env.APOLLO_API_KEY || 'YOUR_APOLLO_API_KEY_HERE'; 
const TARGET_LEADS = 100;

// CSV Writer Setup with EXACT columns requested
const csvWriter = createCsvWriter({
    path: 'Leads_Sheet.csv',
    header: [
        {id: 'companyName', title: 'Company Name'},
        {id: 'domain', title: 'Domain'},
        {id: 'industry', title: 'Industry'},
        {id: 'companySize', title: 'Company Size'},
        {id: 'contactName', title: 'Contact Name'},
        {id: 'jobTitle', title: 'Job Title'},
        {id: 'workEmail', title: 'Work Email'},
        {id: 'linkedinContact', title: 'LinkedIn URL (Contact)'},
        {id: 'linkedinCompany', title: 'LinkedIn URL (Company)'},
        {id: 'source', title: 'Source'},
        {id: 'emailStatus', title: 'Email Status (valid/accept_all/invalid)'},
        {id: 'lastVerified', title: 'Last Verified Date'}
    ]
});

// DELAY FUNCTION (To respect API rate limits)
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function fetchLeads() {
    let allLeads = [];
    let page = 1;
    let uniqueEmails = new Set(); // To ensure no duplicates

    console.log("Starting Mini-Pipeline...");

    try {
        while (allLeads.length < TARGET_LEADS) {
            console.log(`Fetching Page ${page}... (Current Count: ${allLeads.length})`);
            
            const response = await axios.post('https://api.apollo.io/v1/contacts/search', {
                page: page,
                per_page: 50,
                person_titles: ["CEO", "Chief Executive Officer", "COO", "Chief Operating Officer", "CFO", "Chief Financial Officer"],
                organization_num_employees_ranges: ["200,500", "500,1000"],
                person_locations: ["United States", "Canada"],
                q_organization_keyword_tags: ["construction", "architecture & planning", "civil engineering", "commercial real estate", "design"]
            }, {
                headers: { 
                    'Content-Type': 'application/json',
                    'X-Api-Key': API_KEY
                }
            });

            const people = response.data.people;
            
            if (!people || people.length === 0) {
                console.log("No more data found.");
                break;
            }

            // PROCESS & CLEAN DATA
            const processedBatch = people.map(p => {
                const email = p.email || "N/A";
                
                // Skip if no email or if duplicate
                if (email === "N/A" || uniqueEmails.has(email)) return null;
                
                uniqueEmails.add(email);

                return {
                    companyName: p.organization?.name || "N/A",
                    domain: p.organization?.primary_domain || "N/A",
                    industry: p.organization?.industry || "AEC",
                    companySize: p.organization?.estimated_num_employees || "N/A",
                    contactName: `${p.first_name} ${p.last_name}`,
                    jobTitle: p.title,
                    workEmail: email,
                    linkedinContact: p.linkedin_url || "N/A",
                    linkedinCompany: p.organization?.linkedin_url || "N/A",
                    source: "Apollo API",
                    emailStatus: p.email_status || "unknown", 
                    lastVerified: new Date().toISOString().split('T')[0]
                };
            }).filter(item => item !== null); 

            allLeads = [...allLeads, ...processedBatch];
            page++;
            
            // Respect Rate Limits
            await delay(1000);
        }

        // TRIM TO EXACTLY 100
        const finalLeads = allLeads.slice(0, 100);

        // WRITE TO CSV
        await csvWriter.writeRecords(finalLeads);
        console.log(`Success! ${finalLeads.length} leads saved to Leads_Sheet.csv`);

    } catch (error) {
        // Enhanced error logging to see details if it fails again
        console.error("Error:", error.response ? JSON.stringify(error.response.data, null, 2) : error.message);
    }
}

fetchLeads();
