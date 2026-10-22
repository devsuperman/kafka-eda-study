using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace KafkaEdaStudy.Orders.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddSagaState : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "payment_authorized",
                table: "orders",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<bool>(
                name: "stock_reserved",
                table: "orders",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<uint>(
                name: "xmin",
                table: "orders",
                type: "xid",
                rowVersion: true,
                nullable: false,
                defaultValue: 0u);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "payment_authorized",
                table: "orders");

            migrationBuilder.DropColumn(
                name: "stock_reserved",
                table: "orders");

            migrationBuilder.DropColumn(
                name: "xmin",
                table: "orders");
        }
    }
}
