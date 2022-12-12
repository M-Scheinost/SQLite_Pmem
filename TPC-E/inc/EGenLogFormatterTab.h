/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Matt Emmerton
 */

/******************************************************************************
*   Description:        This file implements the methods for formatting
*                       log entries in TSV or CSV format.
******************************************************************************/

#ifndef EGEN_LOG_FORMATTER_H
#define EGEN_LOG_FORMATTER_H

#include <iostream>
#include <iomanip>                              // for log message formatting
#include <sstream>                              // for log message construction

#include "../Utilities/inc/EGenUtilities_stdafx.h"
#include "DriverParamSettings.h"
#include "BaseLogFormatter.h"

namespace TPCE
{

class CLogFormatTab : public CBaseLogFormatter
{
    friend class EGenLogger;

private:
    ostringstream logmsg;
    string        emptyString;

public:

    ////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////

    CLogFormatTab() : emptyString("") {}

    ////////////////////////////////////////////////////////////////
    // CE Transaction Settings
    ////////////////////////////////////////////////////////////////

    string GetLogOutput(CBrokerVolumeSettings& parms);

    string GetLogOutput(CCustomerPositionSettings& parms);

    string GetLogOutput(CMarketWatchSettings& parms);

    string GetLogOutput(CSecurityDetailSettings& parms);

    string GetLogOutput(CTradeLookupSettings& parms);

    string GetLogOutput(CTradeOrderSettings& parms);

    string GetLogOutput(CTradeUpdateSettings& parms);

    ////////////////////////////////////////////////////////////////
    // CE Transaction Mix Settings
    ////////////////////////////////////////////////////////////////

    string GetLogOutput(CTxnMixGeneratorSettings& parms);

    ////////////////////////////////////////////////////////////////
    // Loader Settings
    ////////////////////////////////////////////////////////////////

    string GetLogOutput(CLoaderSettings& parms);

    ////////////////////////////////////////////////////////////////
    // Driver Settings
    ////////////////////////////////////////////////////////////////

    string GetLogOutput(CDriverGlobalSettings& parms);

    string GetLogOutput(CDriverCESettings& parms);

    string GetLogOutput(CDriverCEPartitionSettings& parms);

    string GetLogOutput(CDriverMEESettings& parms);

    string GetLogOutput(CDriverDMSettings& parms);
};

}   // namespace TPCE

#endif //EGEN_LOG_FORMATTER_H
