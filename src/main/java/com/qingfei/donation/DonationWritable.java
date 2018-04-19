package com.qingfei.donation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.util.StringHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ASUS on 2/21/2018.
 */
public class DonationWritable implements WritableComparable<DonationWritable> {
    public String donationId;
    public String projectId;
    public String cartId;
    public String donorCity;
    public String donorState;
    public String donorZip;
    public String donateByTeacher;
    public String donateTimestamp;
    public float dollarAmount;
    public float support;
    public float total;
    public String paymentMethod;
    public String paymentIncAcctCredit;
    public String paymentIncCampaignGiftCard;
    public String paymentIncWebGiftCard;
    public String paymentWasPromoMatched;
    public String isTeacherReferred;
    public String givingPageId;
    public String givingPageType;
    public String forHonoree;
    public String thankYouPacketMailed;

    public int compareTo(DonationWritable o) {
        return this.donationId.compareTo(o.donationId);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(donationId);
        dataOutput.writeUTF(projectId);
        //dataOutput.writeUTF(cartId);
        dataOutput.writeUTF(donorCity);
        dataOutput.writeUTF(donorState);
        //dataOutput.writeUTF(donorZip);
        dataOutput.writeUTF(donateByTeacher);
        dataOutput.writeUTF(donateTimestamp);
        dataOutput.writeFloat(dollarAmount);
        dataOutput.writeFloat(support);
        dataOutput.writeFloat(total);
        /*dataOutput.writeUTF(paymentMethod);
        dataOutput.writeUTF(paymentIncAcctCredit);
        dataOutput.writeUTF(paymentIncCampaignGiftCard);
        dataOutput.writeUTF(paymentIncWebGiftCard);
        dataOutput.writeUTF(paymentWasPromoMatched);
        dataOutput.writeUTF(isTeacherReferred);
        dataOutput.writeUTF(givingPageId);
        dataOutput.writeUTF(givingPageType);
        dataOutput.writeUTF(forHonoree);
        dataOutput.writeUTF(thankYouPacketMailed);*/
    }

    public void readFields(DataInput dataInput) throws IOException {
        donationId=dataInput.readUTF();
        projectId=dataInput.readUTF();
        //cartId=dataInput.readUTF();
        donorCity=dataInput.readUTF();
        donorState=dataInput.readUTF();
        //donorZip=dataInput.readUTF();
        donateByTeacher=dataInput.readUTF();
        donateTimestamp=dataInput.readUTF();
        dollarAmount=dataInput.readFloat();
        support=dataInput.readFloat();
        total = dataInput.readFloat();
        /*paymentMethod=dataInput.readUTF();
        paymentIncAcctCredit=dataInput.readUTF();
        paymentIncCampaignGiftCard=dataInput.readUTF();
        paymentIncWebGiftCard=dataInput.readUTF();
        paymentWasPromoMatched = dataInput.readUTF();
        isTeacherReferred=dataInput.readUTF();
        givingPageId=dataInput.readUTF();
        givingPageType=dataInput.readUTF();
        forHonoree=dataInput.readUTF();
        thankYouPacketMailed=dataInput.readUTF();
        givingPageType=dataInput.readUTF();*/

    }

    public void parseLine(String line) throws IOException {
        line = line.replaceAll("\"","");
        System.out.println(line);
        String[] parts = line.split(",",-1);
        donationId = parts[0];
        projectId = parts[1];
        donorCity = parts[4];
        donorState = parts[5];
        donateByTeacher = parts[7];
        donateTimestamp = parts[8];
        dollarAmount = Float.parseFloat(parts[9]);
        support = Float.parseFloat(parts[10]);
        total = Float.parseFloat(parts[11]);

    }

    @Override
    public String toString() {
        return this.donationId+",["+this.projectId+","+this.donorCity+","+this.donorState+","+this.donateByTeacher+","+this.donateTimestamp+","+this.dollarAmount+","+this.support+","+this.total+"]";
    }
}
